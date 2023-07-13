# Word Count

This Python script performs word counting on a large text file using parallel processing. It splits the input file into multiple partitions, counts the words in each partition using separate processes, and then combines the results to provide a final word count.

## Getting Started

To run the script, follow these steps:

1. Ensure you have Python installed (version 3.6 or higher).
2. Clone the repository.
4. Place the text file you want to count words from in the `data` directory of the project.
5. Open a terminal and navigate to the project directory.
6. Run the script by executing the following command: `python count.py`.

## Example Usage

To count words in a large text file, modify the word_count function call in the __main__ block of the script:

```{python}
if __name__ == "__main__":
    word_count(in_file="data/big.txt", out_file="data/big.json")
```

Replace `data/big.txt` with the path to your input file, and `data/big.json` with the desired output file name.

## Performance

The script utilizes parallel processing to speed up the word counting process. It automatically determines the number of processes to use based on the number of available CPU cores. During execution, the script provides timing information for each major step, allowing you to assess the performance of different stages.
