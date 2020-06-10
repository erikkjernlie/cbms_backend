"""Fast Fourier Transform
"""
from typing import List

import math
import numpy as np
from collections import namedtuple
Variable = namedtuple(
    'Variable',
    ('name', 'valueReference', 'value'),
    defaults=(None, None, 0)
)


class P:
    input_names = ('Displacement [mm]',)
    output_names = ('frequencies',)

    def __init__(self, sample_spacing=0.01, window=300, inputs=['Anne', 0], outputs=['filtered_data', 0]):
        """
        :param sample_spacing: the time to be used between each sample
        :param window: the amount of samples to be used for each calculation
        :param inputs: List of objects on the format [valueReference, name, measurement_ref, measurement_proportion]
        """
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
        self.sample_spacing = float(sample_spacing)
        self.window = int(window)
        self.measurements = np.zeros(self.window, dtype=float)
        self.input_values = [0]
        self.frequencies = np.fft.rfftfreq(self.window, self.sample_spacing)
        self.output_names = tuple(str(f) for f in self.frequencies)
        self.output_values = [np.zeros(self.window, dtype=float)]
        self.t = 0

    def start(self, start_time):
        self.t = start_time

    def set_inputs(self, input_refs: List[int], input_values: List[int]):
        for i in range(len(input_refs)):
            self.input_values[input_refs[i]] = input_values[i]

    def get_outputs(self, output_refs: List[int]):
        self.output_values = np.abs(np.fft.rfft(self.measurements)) ** 2
        return [self.output_values[output_ref] for output_ref in output_refs]

    def step(self, t):
        # print(f'before current_time: {t}, self.time: {self.t}')
        if self.t <= self.sample_spacing and t > 0:
            self.t = t
        # print(f'after current_time: {t}, self.time: {self.t}')
        while t >= self.t or math.isclose(t, self.t, rel_tol=1e-15):
            self.t += self.sample_spacing
            self.measurements[0] = self.input_values[0]
            self.measurements = np.roll(self.measurements, -1)
