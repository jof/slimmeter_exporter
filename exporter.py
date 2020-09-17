import json
import re
from signal import SIGINT, SIGTERM
from socket import (
    SOL_SOCKET,
    SO_KEEPALIVE,
    IPPROTO_TCP,
    TCP_KEEPCNT,
    TCP_KEEPIDLE,
    TCP_KEEPINTVL,
)
from decimal import Decimal

from collections import defaultdict
from circuits import Component, Debugger, Event, Timer, handler
from circuits.io.serial import Serial
from circuits.core.events import started
from circuits.io.events import read, close, ready
from circuits.net.events import connect, disconnect
from circuits.net.sockets import TCPClient

from dsmr_parser.exceptions import ParseError
from dsmr_parser.obis_name_mapping import EN
from dsmr_parser.obis_references import (
    ELECTRICITY_DELIVERED_TARIFF_1,
    ELECTRICITY_DELIVERED_TARIFF_2,
    ELECTRICITY_USED_TARIFF_1,
    ELECTRICITY_USED_TARIFF_2,
)
from dsmr_parser.parsers import TelegramParser
from dsmr_parser.telegram_specifications import V4, V5
from prometheus_client import Counter, Gauge, start_http_server
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, REGISTRY
from threading import Lock


class ensure_connected(Event):
    pass


class read_telegram(Event):
    pass


class TCPClientKeepalive(TCPClient):
    def _create_socket(self):
        sock = super()._create_socket()
        sock.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
        sock.setsockopt(IPPROTO_TCP, TCP_KEEPIDLE, 3)
        sock.setsockopt(IPPROTO_TCP, TCP_KEEPINTVL, 3)
        sock.setsockopt(IPPROTO_TCP, TCP_KEEPCNT, 2)
        return sock


class Exporter(Component):
    channel = "slimmeter_exporter"

    def __init__(self, *args, **kwargs):
        # self._transport_ready = False
        self._receive_buffer = b""
        self._gauges = {}
        self._gauges_lock = Lock()
        self._counters = {}
        self._counters_lock = Lock()
        self._meter_id = None
        self._active_tariff = None
        super(Exporter, self).__init__(*args, **kwargs)
        # Register this with the Prometheus registry.
        # The Registry will call the `collect` method
        REGISTRY.register(self)

    def read(self, incoming_bytes):
        self._receive_buffer += incoming_bytes
        if re.search(b"!\w{4}\r\n", self._receive_buffer):
            self.fire(read_telegram(self._receive_buffer))
            self._receive_buffer = b""

    def collect(self):
        if not self._meter_id and self._active_tariff:
            return None
        # if not self._transport_ready:
        #     return None
        with self._gauges_lock:
            for gauge, value in self._gauges.items():
                gauge = gauge.lower()
                m = GaugeMetricFamily(
                    gauge, gauge, labels=("meter_id", "active_tariff")
                )
                m.add_metric([self._meter_id, self._active_tariff], value)
                yield m
        with self._counters_lock:
            for counter, value in self._counters.items():
                counter = counter.lower()
                m = CounterMetricFamily(
                    counter, counter, labels=("meter_id", "active_tariff")
                )
                m.add_metric([self._meter_id, self._active_tariff], value)
                yield m

    def read_telegram(self, telegram_bytes):
        parser = TelegramParser(V5)
        try:
            telegram = parser.parse(telegram_bytes.decode("utf-8"))
        except ParseError:
            return None
        english_telegram_dict = {}
        for (obis_reference, cosem_object) in telegram.items():
            if obis_reference in EN and hasattr(cosem_object, 'value'):
                english_telegram_dict[EN[obis_reference]] = [
                    cosem_object.value,
                    cosem_object.unit,
                ]

        self._meter_id = str(english_telegram_dict["EQUIPMENT_IDENTIFIER"][0])
        self._active_tariff = str(
            int(english_telegram_dict["ELECTRICITY_ACTIVE_TARIFF"][0], 10)
        )

        for gauge in [
            "CURRENT_ELECTRICITY_DELIVERY",
            "CURRENT_ELECTRICITY_USAGE",
            "INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE",
            "INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE",
            "INSTANTANEOUS_ACTIVE_POWER_L2_NEGATIVE",
            "INSTANTANEOUS_ACTIVE_POWER_L2_POSITIVE",
            "INSTANTANEOUS_ACTIVE_POWER_L3_NEGATIVE",
            "INSTANTANEOUS_ACTIVE_POWER_L3_POSITIVE",
            "INSTANTANEOUS_CURRENT_L1",
            "INSTANTANEOUS_CURRENT_L2",
            "INSTANTANEOUS_CURRENT_L3",
            "INSTANTANEOUS_VOLTAGE_L1",
            "INSTANTANEOUS_VOLTAGE_L2",
            "INSTANTANEOUS_VOLTAGE_L3",
        ]:
            # metric = self._gauges_cache[gauge]
            # metric.labels(meter_id=meter_id, active_tariff=active_tariff).set(
            #     float(english_telegram_dict[gauge][0])
            # )
            self._gauges[gauge] = float(english_telegram_dict[gauge][0])

        for counter in [
            "ELECTRICITY_DELIVERED_TARIFF_1",
            "ELECTRICITY_DELIVERED_TARIFF_2",
            "ELECTRICITY_USED_TARIFF_1",
            "ELECTRICITY_USED_TARIFF_2",
            "LONG_POWER_FAILURE_COUNT",
            "SHORT_POWER_FAILURE_COUNT",
            "VOLTAGE_SAG_L1_COUNT",
            "VOLTAGE_SAG_L2_COUNT",
            "VOLTAGE_SAG_L3_COUNT",
            "VOLTAGE_SWELL_L1_COUNT",
            "VOLTAGE_SWELL_L2_COUNT",
            "VOLTAGE_SWELL_L3_COUNT",
        ]:
            # metric = self._counters_cache[counter]
            # metric.labels(meter_id=meter_id, active_tariff=active_tariff).set(
            #     float(english_telegram_dict[counter][0])
            # )
            self._counters[counter] = float(english_telegram_dict[counter][0])


class ByteStream(Component):
    def read(self, incoming_bytes):
        """
        Connect this ByteStream into the slimmeter_exporter channel
        """
        self.fire(read(incoming_bytes), "slimmeter_exporter")

    def init(self):
        self._ensure_connected_timer = Timer(
            5, ensure_connected(), self.channel, persist=True
        )
        self._ensure_connected_timer.register(self)

    def ensure_connected(self):
        raise NotImplementedError

    def _on_signal(self, event, signo, stack):
        if signo in [SIGINT, SIGTERM]:
            self.fire(close())
            raise SystemExit


class SerialTTY(ByteStream):
    channel = "serial_tty"

    def init(self, port):
        self._opened = False
        self.serial = Serial(port, baudrate=115200, channel=self.channel)
        self.serial.register(self)
        super(SerialTTY, self).init()

    def opened(self, port, baudrate):
        print(f"SerialTTY Opened")
        self._opened = True

    def closed(self):
        print(f"SerialTTY Closed")
        self._opened = False

    def error(self, e):
        self.fire(close())
        print(f"SerialTTY Error: {e!r}")

    def ensure_connected(self):
        if not self._opened:
            self.fire(ready(self))

class SerialTCP(ByteStream):
    channel = "serial_tcp"

    def init(self, host, port):
        self.host = host
        self.port = port
        self.connection = TCPClientKeepalive(channel=self.channel)
        self.connection.register(self)
        super(SerialTCP, self).init()

    def ensure_connected(self):
        if not self.connection.connected:
            self.fire(connect(self.host, self.port))


if __name__ == "__main__":
    start_http_server(9003)
    app = Exporter()
    app += SerialTTY("/dev/smart_meter_p1")
    # app += SerialTCP("chip.flat.jof.io", 31337)
    # app += Debugger()
    app.run()
