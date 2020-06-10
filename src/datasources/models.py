import asyncio
import logging
import struct
import sys
from dataclasses import dataclass, field
from typing import Tuple, List, Optional
import json
from kafka import KafkaProducer
from datetime import datetime
from dateutil.parser import parse

logger = logging.getLogger(__name__)


@dataclass
class UdpDatasource:
    """Represents a single UDP datasource"""
    addr: str
    input_byte_format: str
    input_names: List[str]
    output_refs: List[int]
    time_index: int
    data_type: str
    topic: str = None
    output_names: List[str] = field(init=False)
    byte_format: str = field(init=False)
    output_byte_count: int = field(init=False)
    input_byte_count: int = field(init=False)
    time_bytes_start: int = field(init=False)

    def __post_init__(self):
        self.time_index = self.time_index % len(self.input_byte_format[1:])
        self.input_byte_count = struct.calcsize(self.input_byte_format)
        byte_types = self.input_byte_format[1:]
        self.time_bytes_start = struct.calcsize(byte_types[:self.time_index])
        self.byte_format = self.input_byte_format[0] + byte_types[self.time_index]
        for ref in self.output_refs:
            self.byte_format += byte_types[ref]
        self.output_byte_count = struct.calcsize(self.byte_format)
        self.output_names = [self.input_names[ref] for ref in self.output_refs]


def generate_catman_outputs(output_names: List[str], output_refs, single: bool = False) -> Tuple[
    List[str], List[int], str]:
    """
    Generate ouput setup for a datasource that is using the Catman software

    :param single: true if the data from Catman is single precision (4 bytes each)
    :param output_names: a list of the names of the input data
    """
    byte_format = '<HHI'
    measurement_type = 's' if single else 'd'
    byte_format += (measurement_type * len(output_names))
    output_refs = [ref + 3 for ref in output_refs]
    return ['id', 'channels', 'counter', *output_names], output_refs, byte_format


class UdpReceiver(asyncio.DatagramProtocol):
    """Handles all UDP datasources"""

    def __init__(self, kafka_addr: str):
        """
        Initializes the UdpReceiver with a kafka producer

        :param kafka_addr: the address that will be used to bootstrap kafka
        """
        print("Starting init")
        self.producer = KafkaProducer(bootstrap_servers=kafka_addr)
        self._addr_to_source = {}
        self._available_sources = {}
        self._sources = {}
        self.buffer = bytearray()
        self.buffers = dict()
        self.time_for_last_value = dict()

    def set_source(self,
                   source_id: str,
                   addr: str,
                   topic: str,
                   input_byte_format: str,
                   input_names: List[str],
                   output_refs: List[int],
                   time_index: int,
                   data_type: str
                   ) -> None:
        """
        Creates a new datasource object and adds it to sources, overwriting if necessary

        :param source_id: the id to use for the datasource
        :param addr: the address the datasource will send from
        :param topic: the topic the data will be put on
        :param input_byte_format: the byte_format of the data that will be received
        :param input_names: the names of the values in the data that will be received
        :param output_refs: the indices of the values that will be transmitted to the topic
        :param time_index: the index of the value that represents the time of the data
        """

        source = UdpDatasource(
            input_byte_format=input_byte_format,
            input_names=input_names,
            output_refs=output_refs,
            time_index=time_index,
            addr=addr,
            topic=topic,
            data_type=data_type
        )
        print("addr", addr)
        self._addr_to_source[addr] = source
        self._sources[source_id] = source

    def remove_source(self, source_id) -> None:
        try:
            source: UdpDatasource = self._sources.pop(source_id)
            self._addr_to_source.pop(source.addr)
        except KeyError:
            logger.warning('%s could not be removed since it was not there', source_id)

    def __contains__(self, source_id):
        return source_id in self._sources

    def get_source(self, source_id):
        return self._sources[source_id]

    def get_sources(self):
        """Returns a list of the current sources"""
        return self._sources.copy()

    def get_available_sources(self):
        """Returns a list of the current sources"""
        return self._available_sources.copy()

    def connection_made(self, transport: asyncio.transports.BaseTransport) -> None:
        pass

    def connection_lost(self, exc: Optional[Exception]) -> None:
        pass

    def error_received(self, exc: Exception) -> None:
        logger.exception('error in datasource: %s', exc)

    def datagram_received(self, raw_data: bytes, addr: Tuple[str, int]) -> None:
        """Filters, transforms and buffers incoming packets before sending it to kafka"""
        address = addr[0]
        # print("receiving data", address)
        # print("raw", raw_data)
        # print("address", address)
        # if address == "10.52.204.127":

            # print("RAW DATA", raw_data)
        if address not in self._addr_to_source:
            # used to fetch possible sensor values
            try:
                if (address + "_data") in self._available_sources:
                    self._available_sources[address + "_data"] = self._available_sources[
                                                                     address + "_data"] + raw_data.decode()
                else:
                    self._available_sources[address + "_data"] = raw_data.decode()
                if (address + "_data") in self._available_sources and (
                        self._available_sources[address + "_data"].count("{") == self._available_sources[
                    address + "_data"].count("}")):
                    sensors_data = json.loads(self._available_sources[address + "_data"])
                    sensors = list(sensors_data.keys())
                    self._available_sources[address] = {
                        "sensors": sensors,
                    }
                    del self._available_sources[address + "_data"]

            except ValueError:
                #print("Raw data - no parsing possible")
                pass
        if address in self._addr_to_source:
            source: UdpDatasource = self._addr_to_source[address]
            # print("source.data_type", source.data_type)
            # print("source", source)
            #print("Adress in source", self._addr_to_source)


            # TODO: Nå legger den ikke til data i bufferet ser det ut som
            if source.data_type == "json":
                if address in self.buffers:

                    if "{" in raw_data.decode():
                        # print("this { is here")
                        if "}" in self.buffers[address]:
                            self.buffers[address] = raw_data.decode() + self.buffers[address]
                        else:
                            self.buffers[address] = raw_data.decode() + self.buffers[address]
                    elif "}" in raw_data.decode():
                        if "{" in self.buffers[address]:
                            self.buffers[address] = self.buffers[address] + raw_data.decode()
                        else:
                            self.buffers[address] = self.buffers[address] + raw_data.decode()
                    else:
                        if "{" in self.buffers[address]:
                            self.buffers[address] = self.buffers[address] + raw_data.decode()
                        elif "}" in self.buffers[address]:
                            self.buffers[address] = raw_data.decode() + self.buffers[address]
                        else:
                            self.buffers[address] = self.buffers[address] + raw_data.decode()
                else:
                    print("empty")
                    print("Buffer: ", self.buffers)
                    print("raw data decode", raw_data.decode())
                    self.buffers[address] = ""
                    self.buffers[address] = self.buffers[address] + raw_data.decode()
                    print("self.buffers[address]", self.buffers[address])
                if len(self.buffers[address]) > 0 and \
                        self.buffers[address].count("{") == self.buffers[address].count("}"):
                    try:
                        json_data = json.loads(self.buffers[address])
                        data_values = source.output_names
                        incoming_data = []
                        for (index, value) in enumerate(data_values):
                            # print(value, index)
                            if index == source.time_index:
                                incoming_data.append(
                                    datetime.timestamp(parse(json_data[data_values[source.time_index]])))
                            else:
                                incoming_data.append(float(json_data[value]))

                        data = struct.pack(
                            source.byte_format, incoming_data[source.time_index],
                            *[incoming_data[ref] for ref in source.output_refs])
                        try:
                            last_value = self.time_for_last_value[address]
                            # print(last_value)
                            if datetime.timestamp(parse(json_data[data_values[source.time_index]])) > last_value:
                                self.producer.send(topic=source.topic, value=data)
                                self.buffers[address] = ""
                                self.time_for_last_value[address] = datetime.timestamp(parse(json_data[data_values[source.time_index]]))
                            else:
                                print("skip data due to invalid date")
                                self.buffers[address] = ""
                        except:
                            self.producer.send(topic=source.topic, value=data)
                            self.buffers[address] = ""
                            self.time_for_last_value[address] = datetime.timestamp(parse(json_data[data_values[source.time_index]]))

                    except ValueError:
                        self.buffers[address] = ""
            else:
                # print("source.inout byte formt", source.input_byte_format)
                # print(source.output_byte_count, (len(raw_data), source.input_byte_count))

                data = bytearray(source.output_byte_count * (len(raw_data) // source.input_byte_count))
                for i, msg in enumerate(struct.iter_unpack(source.input_byte_format, raw_data)):
                    data[i:i + source.output_byte_count] = struct.pack(
                        source.byte_format, msg[source.time_index], *[msg[ref] for ref in source.output_refs]
                    )
                if address in self.buffers:
                    self.buffers[address] = self.buffers[address] + data
                else:
                    self.buffers[address] = data
                if len(self.buffers[address]) > len(source.output_names) * 10:
                    try:
                        last_value = self.time_for_last_value[address]
                        # print(last_value)
                        if msg[source.time_index] > last_value:
                            self.producer.send(topic=source.topic, value=self.buffers[address])
                            self.buffers[address] = bytearray()
                            self.time_for_last_value[address] = msg[source.time_index]
                        else:
                            print("skip data due to invalid date")
                            self.buffers[address] = bytearray()
                    except:
                        self.producer.send(topic=source.topic, value=self.buffers[address])
                        self.buffers[address] = bytearray()
                        self.time_for_last_value[address] = msg[source.time_index]



        else:
            logger.debug('%s attempted to send udp data but was not on the list of running datasources', address)
