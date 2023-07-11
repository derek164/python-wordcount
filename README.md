# Word Count

This Python script performs word counting on a large text file using parallel processing. It splits the input file into multiple partitions, counts the words in each partition using separate processes, and then combines the results to provide a final word count.

## Getting Started

To run the script, follow these steps:

1. Ensure you have Python installed (version 3.6 or higher).
2. Clone the repository or download the script file ( word_count.py ).
3. Install the required dependencies by running pip install -r requirements.txt in your terminal.
4. Place the text file you want to count words from in the data directory of the project.
5. Open a terminal and navigate to the project directory.
6. Run the script by executing the following command:

python word_count.py

7. The script will process the input file and generate a count.json file containing the word count.

## Code Overview

The script consists of the following classes and functions:

### WordCount

- __init__(self, file, n=1000) : Initializes the WordCount object with the input file path and the number of lines per partition.
- split_file(self) : Splits the input file into multiple partitions based on the specified number of lines.
- count_partitions(self) : Counts the words in each partition using parallel processing and combines the results.
- count_partition(self, file) : Performs word counting on a single partition.
- __enter__ and __exit__ : Context manager methods to handle file cleanup.

### Partition

- __init__(self, file) : Initializes the Partition object with the file path of a partition.
- line_count(self, line) : Counts the words in a single line of text.
- count(self) : Performs word counting on the partition file.

### word_count

- word_count(in_file, out_file) : Main function that orchestrates the word counting process. It creates a WordCount object, splits the input file, counts the words in each partition, and saves the final word count to a JSON file.

### Other Helper Functions

- timeit(func) : A decorator that measures the execution time of a function.
- grouper(n, iterable, fillvalue=None) : A function that groups elements from an iterable into fixed-length chunks.

## Example Usage

To count words in a large text file, modify the word_count function call in the __main__ block of the script:

if __name__ == "__main__":
    word_count(in_file="data/big.txt", out_file="count.json")

Replace "data/big.txt" with the path to your input file, and "count.json" with the desired output file name.

## Performance

The script utilizes parallel processing with multiple processes to speed up the word counting process. It automatically determines the number of processes to use based on the number of available CPU cores.

During execution, the script provides timing information for each major step, allowing you to assess the performance of different stages.
