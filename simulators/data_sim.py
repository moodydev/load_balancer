from math import ceil, sqrt
from random import choice, gauss, seed


class DataSimulator:
    def __init__(self, cache, seed_num, workers, devices, interval_time):
        self.cache = cache
        self.seed_num = seed_num
        self.workers = workers
        self.devices = devices
        self.interval_time = interval_time

        decimal_points = len([c for c in str(len(self.devices))])
        self.decimal_points = ceil(decimal_points + (decimal_points*5/4))

        self.device_load = round((1/len(devices)), self.decimal_points)
        self.gauss_dev = self.device_load * sqrt(len(self.devices))

    def load_simulator(self):
        device = choice(list(self.devices))

        calc_time = abs(round(gauss(self.device_load, self.gauss_dev), self.decimal_points))
        _key = 'device:{}'.format(device.id_)
        self.cache.increment_field(_key, self.cache.COUNT_FIELD, 1)
        self.cache.increment_field(_key, self.cache.PROC_TIME_FIELD, calc_time)
        self.cache.increment_field(self.cache.SYSTEM_FIELD, self.cache.COUNT_FIELD, 1)

        return calc_time

    def generate_data(self) -> int:
        seed(self.seed_num)
        sleep_time = 0
        while sleep_time < self.interval_time:
            sleep_time += self.load_simulator()
