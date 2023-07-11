import json
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from functools import reduce
from itertools import zip_longest
from multiprocessing import cpu_count
from operator import add
from pathlib import Path

root = Path(__file__).parent


class WordCount:
    def __init__(self, file, n=1000):
        self.file = file
        self.n = n  # lines per partition
        self.temp = root / "data" / "temp"
        self.temp.mkdir(parents=True, exist_ok=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        [f.unlink() for f in self.temp.glob("*")]

    def execute(self):
        self.split()
        counts = self.count_partitions()
        return self.collect(counts)

    def split(self):
        def grouper(n, iterable, fillvalue=None):
            args = [iter(iterable)] * n
            return zip_longest(fillvalue=fillvalue, *args)

        with open(self.file) as f:
            for i, g in enumerate(grouper(self.n, f, fillvalue=""), 1):
                with open(self.temp / f"partition_{i}.txt", "w") as fout:
                    fout.writelines(g)

    def count_partitions(self):
        with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
            futures = [
                executor.submit(self.count_partition, file)
                for file in self.temp.glob("*")
            ]

            counts = []
            for future in as_completed(futures):
                result = future.result()
                counts.append(result)

            return counts

    def count_partition(self, file):
        return Partition(file).count()

    def collect(self, counts):
        return dict(reduce(add, map(Counter, counts)).most_common())


class Partition:
    def __init__(self, file):
        self.file = file

    def line_count(self, line):
        return Counter(line.split())

    def count(self):
        # counter = Counter()
        # with open(self.file) as f:
        #     for line in f:
        #         counter += self.line_count(line)
        # return counter

        counter = Counter()
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            with open(self.file) as f:
                for line in f:
                    futures.append(executor.submit(self.line_count, line))

            for future in as_completed(futures):
                result = future.result()
                counter += result

            return counter


def save_count(count, file):
    with open(file, "w") as f:
        json.dump(count, f, indent=4)


if __name__ == "__main__":
    ts = time.time()
    input_file = "data/big.txt"
    with WordCount(input_file) as counter:
        count = counter.execute()
    # print(count)
    print(time.time() - ts)
    save_count(count, "count.json")
