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

    def execute(self):
        self.split()
        with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
            futures = [
                executor.submit(self.count_partition, file)
                for file in self.temp.glob("*")
            ]

            counts = []
            for future in as_completed(futures):
                result = future.result()
                counts.append(result)
        return self.collect(counts)

    def split(self):
        def grouper(n, iterable, fillvalue=None):
            args = [iter(iterable)] * n
            return zip_longest(fillvalue=fillvalue, *args)

        with open(self.file) as f:
            self.temp.mkdir(parents=True, exist_ok=True)
            [f.unlink() for f in self.temp.glob("*") if f.is_file()]
            for i, g in enumerate(grouper(self.n, f, fillvalue=""), 1):
                with open(self.temp / f"partition_{i}.txt", "w") as fout:
                    fout.writelines(g)

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
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            with open(self.file) as f:
                for line in f:
                    futures.append(executor.submit(self.line_count, line))

            line_counts = []
            for future in as_completed(futures):
                result = future.result()
                line_counts.append(result)

            return dict(reduce(add, map(Counter, line_counts)))


def save_count(count, file):
    with open(file, "w") as f:
        json.dump(count, f, indent=4)


if __name__ == "__main__":
    # ts = time.time()
    count = WordCount("data/big.txt").execute()
    # print(count)
    # print(time.time() - ts)
    save_count(count, "count.json")
