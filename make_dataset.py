"""
Module for creating pandas dataframes from cleaned raw data files. Stores
the data in CSV format in data/clean.
"""
from pathlib import Path
import pandas as pd

def clean(file_path):
    """
    Given the file path object of a raw data file, return a clean string
    with all excess punctuation, whitespace, etc. removed. Should only 
    be a string with words delimited by a single space. 
        Arguments:
            (Path) file_path: the Path object of the file being cleaned
        Returns:
            (string) output: cleaned string 
    """
    ...

def main():
    data_dir = Path().cwd() / "data"
    clean_dir = Path().cwd() / "clean"
    list_of_files = ... #Get a list of all files in data and its subdirectories (use os)
    text = []
    bill_ids = []

    for file in list_of_files:
        text.append(clean(file))
        bill_id = ... #The bill id is the name of the file. Use os to get string name of file
        bill_ids.append(bill_id)

    data = pd.DataFrame(columns={"bill_id": bill_ids, "text": text})
    


if __name__ == "__main__":
    main()
