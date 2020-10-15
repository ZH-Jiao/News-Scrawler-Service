import timeit


class TimeMeasure:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args):
        start = timeit.default_timer()
        res = self.func(*args)
        end = timeit.default_timer()
        print(f'Ran {str(self.func)} in {end - start} seconds')
        return res
