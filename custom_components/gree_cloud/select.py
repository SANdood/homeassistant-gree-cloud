"""Support for Gree Cloud select entities."""

from __future__ import annotations

import logging

from greeclimate_davo22.device import Props

from homeassistant.components.select import SelectEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import DISPATCH_DEVICE_DISCOVERED, DRY_MODES, DRY_MODES_REVERSE
from .coordinator import CloudDeviceDataUpdateCoordinator, GreeCloudConfigEntry, is_hwhp_device
from .entity import GreeCloudEntity

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: GreeCloudConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up Gree Cloud select entities from a config entry."""

    @callback
    def init_device(coordinator: CloudDeviceDataUpdateCoordinator) -> None:
        """Register the device's selects, where the unit reports them."""
        if is_hwhp_device(coordinator):
            return
        if coordinator.device.get_property(Props.DEHUMIDIFIER_MODE) is None:
            return
        async_add_entities([GreeCloudDryModeSelect(coordinator)])

    for coordinator in entry.runtime_data.coordinators:
        init_device(coordinator)

    entry.async_on_unload(
        async_dispatcher_connect(hass, DISPATCH_DEVICE_DISCOVERED, init_device)
    )


class GreeCloudDryModeSelect(GreeCloudEntity, SelectEntity):
    """The dry attribute (Dmod): off, normal, continuous or smart.

    The unit decides which values apply in which mode (Gree+ offers Smart in
    Cool, Normal and Continuous in Dry); this entity writes what it is given.
    Normal in Dry forces the setpoint to 16 C on the units observed.
    """

    _attr_name = "Dry mode"
    _attr_options = list(DRY_MODES)

    def __init__(self, coordinator: CloudDeviceDataUpdateCoordinator) -> None:
        """Initialize the select."""
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.device.device_info.mac}_dry_mode"

    @property
    def current_option(self) -> str | None:
        """Return the unit's dry attribute."""
        raw = self.coordinator.device.get_property(Props.DEHUMIDIFIER_MODE)
        return DRY_MODES_REVERSE.get(raw)

    async def async_select_option(self, option: str) -> None:
        """Write the dry attribute."""
        _LOGGER.debug(
            "Setting dry mode to %s for %s",
            option,
            self.coordinator.device.device_info.name,
        )
        self.coordinator.device.set_property(
            Props.DEHUMIDIFIER_MODE, DRY_MODES[option]
        )
        await self.coordinator.push_state_update()
        self.async_write_ha_state()
