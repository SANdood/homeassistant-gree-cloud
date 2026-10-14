"""Helper and wrapper classes for Gree Cloud module."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass
from datetime import timedelta
import logging
from time import monotonic
from typing import Any

from greeclimate_davo22.cloud_api import CloudDeviceInfo, GreeCloudApi
from greeclimate_davo22.cloud_device import CloudDevice
from greeclimate_davo22.device import (
    TEMP_MAX_TABLE,
    TEMP_MIN_TABLE,
    TEMP_OFFSET,
    Props,
    TemperatureUnits,
)
from greeclimate_davo22.deviceinfo import DeviceInfo
from greeclimate_davo22.mqtt_client import GreeMqttClient

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

# Imported lazily at call time to avoid circular import (coordinator <- __init__).
_RECONNECT_FUNC_NAME = "async_reconnect_mqtt"
_RECONNECT_MODULE = __name__.rsplit(".", 1)[0]  # custom_components.gree_cloud

from .const import (
    CONF_SERVER,
    DISPATCH_DEVICE_DISCOVERED,
    DOMAIN,
    HWHP_PROP_POW_CONSUMP,
    HWHP_PROP_SET_TEM_DEC,
    HWHP_PROP_SET_TEM_INT,
    HWHP_PROP_WATER_TEMP,
    HWHP_PROP_WSTATE,
    MAX_ERRORS,
    PROP_COMPRESSOR_FREQ,
    PROP_ENERGY_TOTAL,
    STALE_AFTER,
    UPDATE_INTERVAL,
)

_LOGGER = logging.getLogger(__name__)

# Extra raw properties requested from the device in addition to the standard Props enum.
# These cover Hot Water Heat Pump (HWHP) devices that expose different sensor keys.
_STANDARD_PROPS: list[str] = [x.value for x in Props]
_HWHP_EXTRA_PROPS = [
    HWHP_PROP_WATER_TEMP,
    HWHP_PROP_SET_TEM_INT,
    HWHP_PROP_SET_TEM_DEC,
    HWHP_PROP_WSTATE,
    HWHP_PROP_POW_CONSUMP,
]

# Extra properties reported by AC units, surfaced by the sensor platform. The
# cloud serves these even though the local UDP protocol does not, so they are
# only available on this integration.
_SENSOR_EXTRA_PROPS = [PROP_ENERGY_TOTAL, PROP_COMPRESSOR_FREQ]

# Key under which the coordinator's snapshot lists stale properties. Not a
# device property; nothing reads coordinator.data, it only drives updates.
_STALE_DATA_KEY = "_stale"


class HWHPAwareCloudDevice(CloudDevice):
    """CloudDevice subclass that also requests HWHP-specific properties.

    The Gree WHIO Hot Water Heat Pump reports current water temperature under
    the ``WatTem`` property key which is not part of the standard ``Props``
    enum.  This subclass overrides ``update_state`` to include that key in the
    status request so it is stored in ``raw_properties`` and can be read by the
    water_heater entity.

    The same mechanism requests ``ElcAll`` (cumulative energy) for AC units,
    read by the sensor platform.

    It also records when each property was last reported, so measured values
    the unit stops sending read unknown instead of being held (see
    ``STALE_AFTER``), and it replaces the library's ``current_temperature``.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the device and its per-property report times."""
        super().__init__(*args, **kwargs)
        self._reported_at: dict[str, float] = {}

    def handle_state_update(self, **kwargs: Any) -> None:
        """Record when each reported property arrived, then apply the update.

        Every packet passes through here: poll replies and unsolicited status
        pushes alike.
        """
        now = monotonic()
        for key in kwargs:
            self._reported_at[key] = now
        # The library settles the TemSen encoding (+40 offset or raw) on the
        # first packet that carries TemSen, even when it reads 0, and never
        # looks again; a raw-encoding unit whose first reading is 0 is then
        # decoded with the offset for good (greeclimate_davo22 2.3.0
        # device.py:307-314, cmroche/greeclimate#196). Wait for a non-zero one.
        defer_check = self.check_version and not kwargs.get(Props.TEMP_SENSOR.value)
        super().handle_state_update(**kwargs)
        if defer_check:
            self.check_version = True

    def is_fresh(self, key: str) -> bool:
        """Return whether the unit reported this property within STALE_AFTER."""
        reported = self._reported_at.get(key)
        return reported is not None and monotonic() - reported <= STALE_AFTER

    def stale_keys(self) -> list[str]:
        """Return the properties not reported within STALE_AFTER."""
        now = monotonic()
        return sorted(
            key for key, at in self._reported_at.items() if now - at > STALE_AFTER
        )

    @property
    def current_temperature(self) -> int | None:
        """Return the unit's own temperature sensor (TemSen), or None.

        Replaces greeclimate_davo22 2.3.0 Device.current_temperature
        (device.py:473-488), which makes the reading echo the setpoint twice:

        - it returns target_temperature whenever TemSen is missing, reads 0 on
          +40-offset firmware, or is out of range, so "no reading" looks
          exactly like a reading equal to the setpoint;
        - in Fahrenheit it decodes TemSen (whole degrees Celsius) through the
          setpoint's TemRec bit, so one 24 C reading shows as 75 F or 76 F
          depending on whether the setpoint is 75 or 76.

        None (unknown) means no usable reading: TemSen never reported, not
        reported within STALE_AFTER, 0 on offset firmware (no sensor), or out
        of range. The encoding is the library's: raw on version-4 firmware
        (from hid, or inferred from a first non-zero reading below 40), else
        offset by +40.
        """
        raw = self.get_property(Props.TEMP_SENSOR)
        if raw is None or not self.is_fresh(Props.TEMP_SENSOR.value):
            return None
        major = self.version and int(self.version.split(".")[0])
        if major == 4:
            celsius = raw
        elif raw != 0:
            celsius = raw - TEMP_OFFSET
        else:
            return None
        if not TEMP_MIN_TABLE <= celsius <= TEMP_MAX_TABLE:
            _LOGGER.warning(
                "Ignoring out-of-range TemSen %s from %s", raw, self.device_info.name
            )
            return None
        if self.temperature_units == TemperatureUnits.F:
            return round(celsius * 9 / 5 + 32)
        return celsius

    async def bind(self, key: str = None, cipher=None):
        """Bind to the device, tolerating a unit that is silent at bind time.

        update_state() below raises on a status timeout (#41). bind() calls it
        during discovery and after an MQTT reconnect, where a unit that happens
        to be offline must not abort setup or the reconnect; the coordinator's
        next poll counts the miss instead.
        """
        try:
            await super().bind(key, cipher)
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "No initial state from %s at bind; the next poll will retry",
                self.device_info.name,
            )

    async def update_state(self) -> None:
        """Update device state, including HWHP-specific properties.

        Raises asyncio.TimeoutError when the device does not answer, so the
        coordinator counts the miss and marks entities unavailable after
        MAX_ERRORS consecutive misses (#41).
        """
        _LOGGER.debug(
            "Updating HWHP-aware cloud device state: %s", self.device_info.name
        )

        props: list[str] = _STANDARD_PROPS + _HWHP_EXTRA_PROPS + _SENSOR_EXTRA_PROPS
        if not self.hid:
            props.append("hid")

        self._response_event = asyncio.Event()
        self._response_data = None

        command = {"t": "status", "cols": props}

        await self._mqtt_client.publish_command(
            self._parent_mac,
            command,
            self.device_cipher,
            self._child_mac,
        )

        try:
            await asyncio.wait_for(
                self._response_event.wait(), timeout=self._command_timeout
            )
            if self._response_data:
                self.handle_state_update(**self._response_data)
            else:
                # The library also sets the event for a reply that carries no
                # state (a late command ack, a pack it could not decrypt or
                # parse). Nothing is updated; values age out via STALE_AFTER.
                _LOGGER.debug(
                    "Status reply from %s carried no state", self.device_info.name
                )
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "Timeout waiting for state update from %s", self.device_info.name
            )
            raise
        finally:
            self._response_event = None
            self._response_data = None


def is_hwhp_device(coordinator: "CloudDeviceDataUpdateCoordinator") -> bool:
    """Return True if the device appears to be a Hot Water Heat Pump.

    Detection requires a positive WatTmp raw value (actual = raw - 100).
    Standard AC units return 0 for unknown properties; a real HWHP reports
    actual water temperature (40–80 °C → raw 140–180), so raw > 0 is the
    discriminator.
    """
    raw = coordinator.device.raw_properties.get(HWHP_PROP_WATER_TEMP)
    return raw is not None and raw > 0


def _is_mqtt_disconnected(error: Exception) -> bool:
    """Return True if *error* indicates the MQTT client is not connected."""
    msg = str(error).lower()
    return any(m in msg for m in ("code:4", "not currently connected", "not connected"))


async def _try_reconnect(hass: HomeAssistant, entry: "GreeCloudConfigEntry") -> bool:
    """Lazy-import and call async_reconnect_mqtt to avoid circular imports."""
    import importlib
    mod = importlib.import_module(_RECONNECT_MODULE)
    return await mod.async_reconnect_mqtt(hass, entry)


type GreeCloudConfigEntry = ConfigEntry[GreeCloudRuntimeData]


@dataclass
class GreeCloudRuntimeData:
    """Runtime data for Gree Climate Cloud integration."""

    cloud_api: GreeCloudApi
    mqtt_client: GreeMqttClient
    coordinators: list[CloudDeviceDataUpdateCoordinator]
    mqtt_reconnect_lock: asyncio.Lock = None

    def __post_init__(self) -> None:
        """Initialise fields that need a running event loop."""
        if self.mqtt_reconnect_lock is None:
            self.mqtt_reconnect_lock = asyncio.Lock()


class CloudDeviceDataUpdateCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Manages polling for state changes from cloud devices."""

    config_entry: GreeCloudConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: GreeCloudConfigEntry,
        device: CloudDevice,
    ) -> None:
        """Initialize the cloud data update coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=f"{DOMAIN}-{device.device_info.name}",
            update_interval=timedelta(seconds=UPDATE_INTERVAL),
            always_update=False,
        )
        self.device = device
        self._error_count: int = 0

    def _snapshot(self) -> dict[str, Any]:
        """Copy the device state, including which measured values are stale.

        With always_update=False an unchanged snapshot skips the entity
        updates, so a value that merely ages out would never be shown as
        unknown; listing the stale keys makes that a change.
        """
        data = copy.deepcopy(self.device.raw_properties)
        if isinstance(self.device, HWHPAwareCloudDevice):
            data[_STALE_DATA_KEY] = self.device.stale_keys()
        return data

    async def _async_update_data(self) -> dict[str, Any]:
        """Update the state of the device from cloud."""
        _LOGGER.debug(
            "Updating cloud device state: %s, error count: %d",
            self.name,
            self._error_count,
        )
        try:
            await self.device.update_state()
            self._error_count = 0
            return self._snapshot()

        except asyncio.TimeoutError as error:
            self._error_count += 1
            if self._error_count >= MAX_ERRORS:
                _LOGGER.warning(
                    "Cloud device %s is unavailable after %d timeouts",
                    self.name,
                    self._error_count,
                )
                raise UpdateFailed(
                    f"Cloud device {self.name} is unavailable, timeout"
                ) from error
            # Return last known state if within error threshold
            return self._snapshot()

        except Exception as error:
            if _is_mqtt_disconnected(error):
                _LOGGER.warning(
                    "MQTT disconnected while updating %s — triggering reconnect",
                    self.name,
                )
                reconnected = await _try_reconnect(self.hass, self.config_entry)
                if reconnected:
                    try:
                        await self.device.update_state()
                        self._error_count = 0
                        return self._snapshot()
                    except Exception as retry_error:
                        _LOGGER.warning(
                            "State update failed after reconnect for %s: %s",
                            self.name,
                            retry_error,
                        )

            self._error_count += 1
            _LOGGER.exception("Error updating cloud device %s: %s", self.name, error)
            if self._error_count >= MAX_ERRORS:
                raise UpdateFailed(
                    f"Cloud device {self.name} failed to update"
                ) from error
            return self._snapshot()

    async def push_state_update(self):
        """Send state updates to the cloud device."""
        try:
            return await self.device.push_state_update()
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "Timeout sending state update to cloud device: %s", self.name
            )
        except Exception as error:
            if _is_mqtt_disconnected(error):
                _LOGGER.warning(
                    "MQTT disconnected while pushing state to %s — triggering reconnect",
                    self.name,
                )
                reconnected = await _try_reconnect(self.hass, self.config_entry)
                if reconnected:
                    try:
                        return await self.device.push_state_update()
                    except Exception as retry_error:
                        _LOGGER.warning(
                            "Push state failed after reconnect for %s: %s",
                            self.name,
                            retry_error,
                        )
                        return
            _LOGGER.exception(
                "Error sending state update to cloud device %s: %s", self.name, error
            )


class CloudDiscoveryService:
    """Cloud discovery service for Gree devices."""

    def __init__(
        self, hass: HomeAssistant, entry: GreeCloudConfigEntry, api: GreeCloudApi
    ) -> None:
        """Initialize cloud discovery service."""
        self.hass = hass
        self.entry = entry
        self.api = api

    async def discover_devices(
        self, mqtt_client: GreeMqttClient
    ) -> list[CloudDeviceDataUpdateCoordinator]:
        """Discover all cloud devices."""
        coordinators = []

        try:
            # Get all devices from cloud
            _LOGGER.debug("Fetching devices from Gree Cloud")
            cloud_devices = await self.api.get_all_devices()

            _LOGGER.info("Found %d cloud devices", len(cloud_devices))

            # Create coordinator for each device
            for cloud_dev_info in cloud_devices:
                try:
                    # Create DeviceInfo for CloudDevice
                    device_info = DeviceInfo(
                        ip="0.0.0.0",  # Not used for cloud devices
                        port=0,  # Not used for cloud devices
                        mac=cloud_dev_info.mac,
                        name=cloud_dev_info.name,
                    )

                    # Create cloud device instance
                    device = HWHPAwareCloudDevice(
                        mqtt_client=mqtt_client,
                        device_info=device_info,
                        device_key=cloud_dev_info.key,
                        cipher_version=1,  # Default to v1, can be made configurable
                    )

                    # Bind to cloud device (subscribe to MQTT topics)
                    await device.bind()

                    _LOGGER.debug(
                        "Bound to cloud device: %s (MAC: %s)",
                        device.device_info.name,
                        device.device_info.mac,
                    )

                    # Create coordinator
                    coordinator = CloudDeviceDataUpdateCoordinator(
                        self.hass, self.entry, device
                    )
                    coordinators.append(coordinator)

                    # Initial refresh
                    await coordinator.async_config_entry_first_refresh()

                    # Notify about discovered device
                    async_dispatcher_send(
                        self.hass, DISPATCH_DEVICE_DISCOVERED, coordinator
                    )

                except Exception as err:
                    _LOGGER.exception(
                        "Failed to setup cloud device %s: %s",
                        cloud_dev_info.name,
                        err,
                    )

        except Exception as err:
            _LOGGER.exception("Failed to discover cloud devices: %s", err)

        return coordinators
