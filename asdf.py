import importlib

import matplotlib.pyplot as plt
import numpy as np
from scipy import signal as si


def generate_signal(frequencies, magnitudes):
    def s(t):
        return sum([
            magnitudes[i] * np.sin(frequencies[i] * (2 * np.pi * t))
            for i in range(len(frequencies))
        ])

    return s


def generate_noise(magnitude):
    def noise(t):
        return (np.random.random() - .5) * 2 * magnitude

    return noise


sample_spacing = 1 / 600
start_time = 0
end_time = 5
order = 10
buffer_size = 200
cutoff_frequency = 10
btype = 'hp'
if __name__ == '__main__':
    # plt.figure(dpi=1200)

    f = importlib.import_module('files.blueprints.butterworth.main').P(start_time, sample_spacing, buffer_size, cutoff_frequency)
    signal = generate_signal([1, 2, 4, 8, 16, 32, 64, 128], [0, 0, 0, 0, 1, 0, 0, 0])
    noise = generate_noise(.0)
    t_range = np.arange(start_time, end_time, sample_spacing)
    output = np.zeros(len(t_range))
    plt.plot(t_range, [signal(t) + noise(t) for t in t_range])
    plt.show()
    sos = si.butter(order, cutoff_frequency, btype, fs=1 / sample_spacing, output='sos')
    filtered = si.sosfilt(sos, [signal(t) + noise(t) for t in t_range])
    plt.plot(t_range, filtered)
    plt.show()
    # asdf = np.zeros(len(t_range))
    # for i in range(len(t_range)//buffer_size):
    #     asdf[i:buffer_size] =

    for i, t in enumerate(t_range):
        f.set_inputs([0], [signal(t) + noise(t)])
        f.step(t)
        output[i] = f.get_outputs([0])[0]
    plt.plot(t_range, output)
    plt.show()

