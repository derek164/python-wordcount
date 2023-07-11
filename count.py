import re
import json
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import wraps
from itertools import islice
from multiprocessing import cpu_count
from operator import add
from pathlib import Path

root = Path(__file__).parent


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed = format(end_time - start_time, ".4f")
        template = "Function: {name:<30} Time (s): {elapsed:<10}"
        print(template.format(name=func.__name__, elapsed=elapsed))
        return result

    return timeit_wrapper


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

    @timeit
    def split_file(self):
        def grouper(n, iterable):
            iterable = iter(iterable)
            return iter(lambda: list(islice(iterable, n)), [])

        with open(self.file) as in_file:
            for i, group in enumerate(grouper(self.n, in_file), 1):
                with open(
                    self.temp / f"partition_{format(i, '06d')}.txt", "w"
                ) as partition:
                    partition.writelines(group)

    @timeit
    def count_partitions(self):
        counter = Counter()
        with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
            futures = [
                executor.submit(self.count_partition, file)
                for file in self.temp.glob("*")
            ]

            for future in as_completed(futures):
                partition_count = future.result()
                counter += partition_count

            return dict(counter.most_common())

    def count_partition(self, file):
        counter = Counter()
        with open(file) as f:
            for line in f:
                line = re.sub(r'[^\w\s\-]', '', line)
                counter += Counter(line.lower().split())
        return counter


@timeit
def word_count(in_file, out_file):
    def save_count(count, out_file):
        with open(out_file, "w") as f:
            json.dump(count, f, indent=2)

    with WordCount(in_file) as wc:
        wc.split_file()
        count = wc.count_partitions()
        save_count(count, out_file)


if __name__ == "__main__":
    word_count(in_file="data/big.txt", out_file="count.json")
