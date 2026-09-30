#!/usr/bin/env python3
"""
Repeater Telemetry Service for MeshCore Bot (local service plugin)

Polls MeshCore repeaters through the bot's own companion radio (guest login,
then status + telemetry requests) and serves the latest readings as Prometheus
metrics over HTTP. The bot holds the radio's serial port, so this has to run
inside the bot rather than as a separate exporter.

Deploy to <local_dir_path>/service_plugins/ and add to config.ini
(see config.example.ini for every option):

    [RepeaterTelemetry_Service]
    enabled = true
    targets = My Repeater:<64 hex chars of the repeater's public key>
    guest_password =
    interval_seconds = 240
    listen_host = 0.0.0.0
    listen_port = 9108
    # optional: export daylight for a site
    sun_latitude = 51.4779
    sun_longitude = -0.0015
    sun_location = my-site
"""
import asyncio
import contextlib
import math
import time
from typing import Any, Optional

from aiohttp import web
from meshcore import EventType

try:
    import ephem
except ImportError:  # sun metrics are optional; everything else still works
    ephem = None

from modules.service_plugins.base_service import BaseServicePlugin

# Floor for request timeouts. The library's suggested timeout for a 0-hop
# neighbour is short enough that a busy channel can make it expire early.
MIN_REQUEST_TIMEOUT = 8

# (status field, metric suffix, type, help, extra labels, scale)
STATUS_METRICS = [
    ("bat", "battery_volts", "gauge", "Battery voltage reported in status", {}, 0.001),
    ("noise_floor", "noise_floor_dbm", "gauge", "Radio noise floor", {}, 1),
    ("last_rssi", "last_rssi_dbm", "gauge", "RSSI of the last packet the repeater received", {}, 1),
    ("last_snr", "last_snr_db", "gauge", "SNR of the last packet the repeater received", {}, 1),
    ("uptime", "uptime_seconds", "gauge", "Repeater uptime", {}, 1),
    ("tx_queue_len", "tx_queue_len", "gauge", "Outbound queue length", {}, 1),
    ("recv_flood", "packets_received_total", "counter", "Packets received", {"route": "flood"}, 1),
    ("recv_direct", "packets_received_total", "counter", "Packets received", {"route": "direct"}, 1),
    ("sent_flood", "packets_sent_total", "counter", "Packets sent", {"route": "flood"}, 1),
    ("sent_direct", "packets_sent_total", "counter", "Packets sent", {"route": "direct"}, 1),
    ("flood_dups", "duplicates_total", "counter", "Duplicate packets dropped", {"route": "flood"}, 1),
    ("direct_dups", "duplicates_total", "counter", "Duplicate packets dropped", {"route": "direct"}, 1),
    ("recv_errors", "receive_errors_total", "counter", "Packets received with errors", {}, 1),
    ("airtime", "airtime_seconds_total", "counter", "Radio airtime", {"dir": "tx"}, 1),
    ("rx_airtime", "airtime_seconds_total", "counter", "Radio airtime", {"dir": "rx"}, 1),
    ("full_evts", "full_events_total", "counter", "Queue-full events", {}, 1),
]

PREFIX = "meshcore_repeater_"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(labels: dict[str, str]) -> str:
    return "{" + ",".join(f'{k}="{_escape(str(v))}"' for k, v in labels.items()) + "}"


class RepeaterTelemetryService(BaseServicePlugin):
    """Polls repeaters for status/telemetry and exposes them on /metrics."""

    config_section = "RepeaterTelemetry_Service"
    description = "Poll repeaters (guest login) and export status/telemetry to Prometheus"

    def __init__(self, bot: Any):
        super().__init__(bot)
        section = self.config_section
        cfg = bot.config
        self.interval_seconds = max(60, cfg.getint(section, "interval_seconds", fallback=240))
        self.guest_password = cfg.get(section, "guest_password", fallback="").strip()
        self.listen_host = cfg.get(section, "listen_host", fallback="0.0.0.0").strip()
        self.listen_port = cfg.getint(section, "listen_port", fallback=9108)
        self.sun_lat = cfg.get(section, "sun_latitude", fallback="").strip()
        self.sun_lon = cfg.get(section, "sun_longitude", fallback="").strip()
        self.sun_location = cfg.get(section, "sun_location", fallback="").strip() or "site"
        if self.sun_lat and self.sun_lon and ephem is None:
            self.logger.warning("RepeaterTelemetry: sun_latitude set but ephem is not installed; no sun metrics")
        self.targets: list[tuple[str, str]] = []
        for entry in cfg.get(section, "targets", fallback="").split(","):
            name, sep, pubkey = entry.strip().rpartition(":")
            pubkey = pubkey.strip().lower()
            if not sep or not name.strip() or len(pubkey) != 64:
                if entry.strip():
                    self.logger.warning("RepeaterTelemetry: ignoring malformed target %r", entry.strip())
                continue
            self.targets.append((name.strip(), pubkey))
        # pubkey -> latest readings; only filled after a successful poll so a
        # repeater that has never answered exports nothing rather than zeros
        self._status: dict[str, dict[str, Any]] = {}
        self._telemetry: dict[str, list[dict[str, Any]]] = {}
        self._poll_ok: dict[tuple[str, str], int] = {}
        self._last_success: dict[str, float] = {}
        self._poll_task: Optional[asyncio.Task] = None
        self._runner: Optional[web.AppRunner] = None

    async def start(self) -> None:
        if not self.targets:
            self.logger.warning("RepeaterTelemetry: no valid targets configured, not starting")
            return
        self._running = True
        app = web.Application()
        app.router.add_get("/metrics", self._handle_metrics)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        await web.TCPSite(self._runner, self.listen_host, self.listen_port).start()
        self._poll_task = asyncio.create_task(self._poll_loop())
        self.logger.info(
            "RepeaterTelemetry started: %d target(s), every %ds, metrics on %s:%d",
            len(self.targets), self.interval_seconds, self.listen_host, self.listen_port,
        )

    async def stop(self) -> None:
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
            self._poll_task = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        self.logger.info("RepeaterTelemetry stopped")

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                for name, pubkey in self.targets:
                    await self._poll_target(name, pubkey)
                await asyncio.sleep(self.interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("RepeaterTelemetry: poll loop error: %s", e)
                await asyncio.sleep(60)

    async def _poll_target(self, name: str, pubkey: str) -> None:
        # The bot replaces its meshcore object on reconnect, so never cache it
        mc = getattr(self.bot, "meshcore", None)
        if mc is None or not mc.is_connected:
            self.logger.warning("RepeaterTelemetry: radio not connected, skipping %s", name)
            return
        cmds = mc.commands

        if not await self._ensure_zero_hop(cmds, name, pubkey):
            self._poll_ok[(pubkey, "status")] = 0
            self._poll_ok[(pubkey, "telemetry")] = 0
            return

        status = await cmds.req_status_sync(pubkey, min_timeout=MIN_REQUEST_TIMEOUT)
        if status is None:
            # Guest sessions live only in the repeater's RAM; after a repeater
            # reboot (or on first poll) we must log in again before it answers
            login = await cmds.send_login_sync(pubkey, self.guest_password, min_timeout=MIN_REQUEST_TIMEOUT)
            if login is None:
                self.logger.warning("RepeaterTelemetry: %s did not answer status or login", name)
                self._poll_ok[(pubkey, "status")] = 0
                self._poll_ok[(pubkey, "telemetry")] = 0
                return
            self.logger.info(
                "RepeaterTelemetry: logged in to %s (admin=%s)", name, login.payload.get("is_admin")
            )
            status = await cmds.req_status_sync(pubkey, min_timeout=MIN_REQUEST_TIMEOUT)

        self._poll_ok[(pubkey, "status")] = int(status is not None)
        if status is not None:
            self._status[pubkey] = status
            self._last_success[pubkey] = time.time()
        else:
            self.logger.warning("RepeaterTelemetry: %s logged in but status request failed", name)

        lpp = await cmds.req_telemetry_sync(pubkey, min_timeout=MIN_REQUEST_TIMEOUT)
        self._poll_ok[(pubkey, "telemetry")] = int(lpp is not None)
        if lpp is not None:
            self._telemetry[pubkey] = lpp
            self._last_success[pubkey] = time.time()
        else:
            self.logger.warning("RepeaterTelemetry: %s telemetry request failed", name)

        if status is not None:
            self.logger.info(
                "RepeaterTelemetry: %s bat=%.3fV noise=%sdBm rssi=%sdBm snr=%sdB up=%ss",
                name, (status.get("bat") or 0) / 1000, status.get("noise_floor"),
                status.get("last_rssi"), status.get("last_snr"), status.get("uptime"),
            )

    async def _ensure_zero_hop(self, cmds: Any, name: str, pubkey: str) -> bool:
        """Pin the radio's route to this repeater to zero-hop direct.

        The companion firmware sends logins and requests direct when the
        contact has a known path and floods them when it does not
        (BaseChatMesh::sendLogin/sendRequest). A reset or re-learned contact
        would silently turn every poll into a mesh-wide flood, and a multi-hop
        path would carry it through other repeaters. Polls are only meant for
        repeaters the bot hears directly, so pin the path, and skip the poll
        rather than let it leave the neighbourhood.
        """
        ev = await cmds.get_contact_by_key(bytes.fromhex(pubkey))
        if ev is None or ev.type == EventType.ERROR:
            self.logger.warning("RepeaterTelemetry: %s is not in the radio's contacts, skipping", name)
            return False
        contact = ev.payload
        if contact.get("out_path_len") == 0:
            return True
        path_len = contact.get("out_path_len")
        self.logger.warning(
            "RepeaterTelemetry: %s path was %s, pinning to zero-hop direct",
            name, "unknown (polls would flood)" if path_len == -1 else f"{path_len} hop(s)",
        )
        res = await cmds.change_contact_path(
            contact, "", path_hash_mode=max(0, contact.get("out_path_hash_mode", 0))
        )
        if res is None or res.type == EventType.ERROR:
            self.logger.warning("RepeaterTelemetry: could not pin %s to zero-hop, skipping", name)
            return False
        return True

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        return web.Response(text=self._render_metrics(), content_type="text/plain", charset="utf-8")

    def _sun_state(self, unix_time: float) -> Optional[tuple[int, float]]:
        """(daylight 0/1, sun elevation in degrees) at the configured site, or None.

        Daylight uses ephem's standard sunrise/sunset (upper limb on a -0:34
        refraction horizon): the sun is up when the next setting comes before
        the next rising.
        """
        if ephem is None or not (self.sun_lat and self.sun_lon):
            return None
        obs = ephem.Observer()
        obs.lat, obs.lon = self.sun_lat, self.sun_lon
        obs.date = ephem.Date(ephem.Date("1970/1/1") + unix_time / 86400.0)
        sun = ephem.Sun()
        try:
            up = obs.next_setting(sun) < obs.next_rising(sun)
        except (ephem.AlwaysUpError, ephem.NeverUpError) as e:  # polar day/night
            up = isinstance(e, ephem.AlwaysUpError)
        sun.compute(obs)
        return int(up), math.degrees(float(sun.alt))

    def _render_metrics(self) -> str:
        families: dict[str, tuple[str, str, list[str]]] = {}

        def add(suffix: str, mtype: str, help_text: str, labels: dict[str, str], value: float) -> None:
            fam = families.setdefault(PREFIX + suffix, (mtype, help_text, []))
            fam[2].append(f"{PREFIX}{suffix}{_labels(labels)} {value}")

        for name, pubkey in self.targets:
            base = {"repeater": name, "pubkey": pubkey[:8]}
            for kind in ("status", "telemetry"):
                if (pubkey, kind) in self._poll_ok:
                    add("poll_success", "gauge", "1 if the last poll of this kind succeeded",
                        {**base, "kind": kind}, self._poll_ok[(pubkey, kind)])
            if pubkey in self._last_success:
                add("last_success_timestamp_seconds", "gauge", "Unix time of the last successful poll",
                    base, self._last_success[pubkey])
            status = self._status.get(pubkey)
            if status:
                for field, suffix, mtype, help_text, extra, scale in STATUS_METRICS:
                    value = status.get(field)
                    if value is not None:
                        add(suffix, mtype, help_text, {**base, **extra}, value * scale)
            for item in self._telemetry.get(pubkey, []):
                value, ltype = item.get("value"), item.get("type")
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    continue  # skip structured LPP types such as gps
                labels = {**base, "channel": str(item.get("channel"))}
                if ltype == "voltage":
                    add("telemetry_voltage_volts", "gauge", "Voltage from telemetry", labels, value)
                elif ltype == "temperature":
                    add("temperature_celsius", "gauge", "Temperature from telemetry", labels, value)
                else:
                    add("telemetry_value", "gauge", "Other scalar telemetry readings",
                        {**labels, "type": str(ltype)}, value)

        sun = self._sun_state(time.time())
        if sun is not None:
            daylight, elevation = sun
            loc = _labels({"location": self.sun_location})
            families["meshcore_sun_daylight"] = (
                "gauge", "1 between sunrise and sunset at the configured site, else 0",
                [f"meshcore_sun_daylight{loc} {daylight}"])
            families["meshcore_sun_elevation_degrees"] = (
                "gauge", "Sun elevation above the horizon at the configured site",
                [f"meshcore_sun_elevation_degrees{loc} {elevation:.2f}"])

        lines = []
        for fam_name, (mtype, help_text, samples) in families.items():
            lines.append(f"# HELP {fam_name} {help_text}")
            lines.append(f"# TYPE {fam_name} {mtype}")
            lines.extend(samples)
        return "\n".join(lines) + "\n"
