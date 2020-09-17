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


# def gauge_factory(name: str) -> Gauge:
#     name = name.lower()
#     return Gauge(name, name, ("meter_id", "active_tariff"))


# def counter_factory(name: str) -> Counter:
#     name = name.lower()
#     return Counter(name, name, ("meter_id", "active_tariff"))


# class keydefaultdict(defaultdict):
#     def __missing__(self, key):
#         if self.default_factory is None:
#             raise KeyError(key)
#         else:
#             ret = self[key] = self.default_factory(key)
#             return ret


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
        self._transport_ready = False
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
        if not self._transport_ready:
            return None
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
            # print(telegram)
        except ParseError:
            return None
        # print(repr(telegram[ELECTRICITY_USED_TARIFF_1].value))
        # print(repr(telegram[ELECTRICITY_USED_TARIFF_2].value))
        english_telegram_dict = {}
        for (obis_reference, cosem_object) in telegram.items():
            if obis_reference in EN:
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

class SerialTTYExporter(Exporter):
    def init(self, port):
        self.serial = Serial(port, baudrate=115200, channel=self.channel)
        self.serial.register(self)

    def opened(self, port, baudrate):
        self._transport_ready = True

    def closed(self):
        self._transport_ready = False


class SerialTCPExporter(Exporter):
    def __init__(self, serial_host, serial_port):
        self._serial_connection = None
        self._serial_host = serial_host
        self._serial_port = serial_port
        self._ensure_connected_timer = None
        super(SerialTCPExporter, self).__init__(serial_host, serial_port)

    def started(self, manager):
        self.ensure_connected()
        REGISTRY.register(self)

    def connected(self, host, port):
        self._transport_ready = True

    def disconnected(self, host, port):
        self._transport_ready = False

    def ensure_connected(self):
        if not self._serial_connection:
            self._serial_connection = TCPClientKeepalive(channel=self.channel)
            self._serial_connection.register(self)
        if not self._serial_connection.connected:
            self.fire(connect(self._serial_host, self._serial_port))
        if not self._ensure_connected_timer:
            self._ensure_connected_timer = Timer(
                5, ensure_connected(), self.channel, persist=True
            )
            self._ensure_connected_timer.register(self)

    def _on_signal(self, event, signo, stack):
        if signo in [SIGINT, SIGTERM]:
            self._serial_connection.disconnect()


# print(repr(english_telegram_dict))
# print(json.dumps(english_telegram_dict, sort_keys=True, indent=1, default=str))


if __name__ == "__main__":

    # import ptvsd
    # ptvsd.enable_attach(('127.0.0.1', 5678))
    # print("WAIT FOR ATTACH")
    # ptvsd.wait_for_attach()

    start_http_server(9003)
    # app = SerialTCPExporter("192.168.178.62", 31337)
    app = SerialTTYExporter("/dev/smart_meter_p1")
    # app += Debugger()
    app.run()
