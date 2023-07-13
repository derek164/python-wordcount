import json
import re
import tempfile
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import wraps
from itertools import islice
from multiprocessing import cpu_count
from operator import add
from pathlib import Path


def timeit(func):
    """
    Decorator that measures the execution time of a function.

    Parameters
    ----------
    func
        Input function.
    """

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
    """
    Compute word frequency of `.txt` file.

    Parameters
    ----------
    file
        Input file path.
    n
        Number of lines per partition.
    """

    def __init__(self, file, n=800):
        self.file = file
        self.n = n  # lines per partition
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.temp_dir.cleanup()

    @timeit
    def create_partitions(self):
        """
        Splits the input file into partitions with at most `self.n` lines.
        """

        def grouper(n, iterable):
            """
            Groups elements from an iterable into fixed-length chunks.

            Parameters
            ----------
            n
                Chunk size.
            iterable
                Input iterable.
            """

            iterable = iter(iterable)
            return iter(lambda: list(islice(iterable, n)), [])

        with open(self.file) as in_file:
            for i, group in enumerate(grouper(self.n, in_file), 1):
                partition = f"partition_{format(i, '06d')}.txt"
                with open(self.temp_path / partition, "w") as partition:
                    partition.writelines(group)

    @timeit
    def count_partitions(self):
        """
        Counts the words in each partition using parallel processing and combines the results.
        """

        counter = Counter()
        with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
            futures = [
                executor.submit(self.count_partition, file)
                for file in self.temp_path.glob("*")
            ]

            for future in as_completed(futures):
                partition_count = future.result()
                counter += partition_count

            return dict(counter.most_common())

    def count_partition(self, file):
        """
        Counts the words a single partition given its file path.

        file
            Partition file path.
        """

        counter = Counter()
        with open(file) as f:
            for line in f:
                counter += self.count_line(line)
        return counter

    def count_line(self, line):
        """
        Counts the words in a single line of text after regex text cleaning.
        """

        line = re.sub(r"\-{2}", " ", line)
        line = re.sub(r"(\d+)\-(\d+)", r"\1 \2", line)
        line = re.sub(r"[^a-z\d\s\-\'\"]", " ", line.lower())
        line = [x.strip("-'\"") for x in line.split()]
        return Counter(line)


@timeit
def word_count(in_file, out_file):
    """
    Driver method for orchestrating the word counting process

    in_file
        Input file to count.
    out_file
        Output file to save count.
    """

    def save_count(count, out_file):
        """
        Save word count results to output file

        count
            Word count results.
        out_file
            Output file.
        """

        with open(out_file, "w") as f:
            json.dump(count, f, indent=2)

    in_file = Path(in_file)
    name, bytes = in_file.name, in_file.stat().st_size
    print(f": {name} ({bytes:,}) :".center(57, "-"))

    with WordCount(in_file) as wc:
        wc.create_partitions()
        count = wc.count_partitions()
        save_count(count, out_file)


if __name__ == "__main__":
    word_count(in_file="data/small.txt", out_file="data/small.json")
    word_count(in_file="data/big.txt", out_file="data/big.json")
    # word_count(in_file="data/enwik8.txt", out_file="data/enwik8.json")
