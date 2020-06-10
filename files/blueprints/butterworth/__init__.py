"""Butterworth filter
"""
from typing import List
import math
import numpy as np
import scipy.signal as si
from collections import namedtuple
Variable = namedtuple(
    'Variable',
    ('name', 'valueReference', 'value'),
    defaults=(None, None, 0)
)


class P:
    input_names = ('Displacement [mm]',)
    output_names = ('output_name',)

    def __init__(self, sample_spacing=20, buffer_size=50, cutoff_frequency=5, btype='hp', order=10,
                 inputs=['Anne', 0], outputs=['filtered_data', 0]):
        self.inputs = [
            Variable(
                name=v['name'],
                valueReference=v['valueReference'],
                value=0
            )
            for v in inputs
        ]
        self.outputs = [
            Variable(
                valueReference=v['valueReference'],
                name=v['name'],
                value=0
            )
            for v in outputs
        ]
        self.t = 0
        self.sample_spacing = float(1/sample_spacing)
        self.buffer_size = int(buffer_size)
        self.buffers = np.zeros((2, self.buffer_size))
        self.time_buffers = np.zeros((2, self.buffer_size))
        self.current_buffer = 0
        self.output_buffer = np.zeros(self.buffer_size)
        self.index = -1
        print("Er det her det skjer igjen?")
        print("order", order, "cutoff", cutoff_frequency, btype, "sample_spacing", sample_spacing)
        print("order", type(order), "cutoff", type(cutoff_frequency), type(btype), "sample_spacing", type(sample_spacing))
        self.sos = si.butter(N=order, Wn=cutoff_frequency, btype=btype, fs=sample_spacing, output='sos')
        print("nei det er det ikke")
        self.last_value = 0

    def start(self, start_time):
        self.t = start_time

    def set_inputs(self, input_refs: List[int], input_values: List[int]):
        if len(input_refs) == 1:
            self.last_value = input_values[0]

    def get_outputs(self, output_refs: List[int]):
        if len(output_refs) == 1:
            return [self.output_buffer[self.index]]

    def get_time(self):
        return self.time_buffers[(self.current_buffer + 1) % 2, self.index]

    def step(self, t):
        print("t", t, "last value", self.last_value)
        if self.t <= self.sample_spacing and t > 0:
            self.t = t
        while t >= self.t or math.isclose(t, self.t, rel_tol=1e-15):
            self.t += self.sample_spacing
            self.index += 1
            if self.index >= self.buffer_size:
                next_buffer = (self.current_buffer + 1) % 2
                self.output_buffer = si.sosfilt(
                    self.sos,
                    np.concatenate((
                        self.buffers[next_buffer],
                        self.buffers[self.current_buffer]
                    )))[self.buffer_size:]
                self.index = 0
                self.current_buffer = next_buffer
            self.buffers[self.current_buffer, self.index] = self.last_value
            self.time_buffers[self.current_buffer, self.index] = self.t
