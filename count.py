import json
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import reduce
from operator import add


def line_count(line):
    return Counter(line.split())


def word_count(file):
    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = []
        with open(file) as f:
            for line in f:
                futures.append(executor.submit(line_count, line))

        line_counts = []
        for future in as_completed(futures):
            result = future.result()
            line_counts.append(result)

        return dict(reduce(add, map(Counter, line_counts)).most_common())


def save_count(count, file):
    with open(file, "w") as f:
        json.dump(count, f, indent=4)


if __name__ == "__main__":
    count = word_count("sample.txt")
    save_count(count, "count.json")
