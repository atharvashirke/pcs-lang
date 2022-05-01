"""
A module to scrape all bill text off of the California Legislation. Stores 
all bills as their bill ID in files within data/raw.
"""

import requests
import os
from pathlib import Path
from bs4 import BeautifulSoup
import time 
import concurrent.futures
from random import randint

def clean_data(result_set: list) -> str:
    """ 
        Cleans inputted strings of all html/css and other markup text.

        Arguments: 
            (list) result_set : Set of strings to clean from soup
        Returns 
            (string) output: output string cleaned of all html/css formatting
    """
    output = []

    for item in result_set:
        bill_text = item.get_text()
        bill_text = bill_text[bill_text.index("#DOCUMENTBill Start") + len("#DOCUMENTBill Start"):]
        output.append(bill_text.strip())

    return ''.join(output)

def download_bill(bill_url, data_path: Path):
    """
        Download content of bill from bill_url and store in the directory
        given by data_path.

        Arguments:
            (string) bill_url: slug url for given bill
            (Path) data_path: dir for storing scraped file
        Returns:
            None
    """
    url_start = "https://leginfo.legislature.ca.gov"

    if "bill_id=" in bill_url:
        bill_id = bill_url[bill_url.index("bill_id=") + len("bill_id="):] + ".txt"
        bill_path = data_path / bill_id

        if not bill_path.exists():
            bill_page = requests.get(url_start + bill_url)
            bill_soup = BeautifulSoup(bill_page.content, "html.parser")

            with open(bill_path, 'w+') as df:
                df.write(clean_data(bill_soup.find_all(id="content_main")))
        
            time.sleep(randint(1, 3))

def get_bill_urls(directory_url):
    """
        Returns a list with all urls from a "directory page": a page on 
        CA legislator that lists all bills in a given year.

        Arguments: 
            (str) directory_url: the url of a "directory page"
        Returns:
            (list) urls: urls to all bills on a directory page
    """
    page = requests.get(directory_url)
    soup = BeautifulSoup(page.content, "html.parser")

    urls = []

    for item in soup.find_all('a', href=True):
        urls.append(item['href'])
    
    return urls

def download_directory(directory_url, year_dir, year):
    """
        Downloads all files linked in the given directory url.
        Stores the data in the given directory path.

        Arguments: 
            (string) directory_url: url for directory page with links
            (Path) year_dir: path object representing the "year" directory these files 
            are written in
            (int) year: used for name scheme of files
        Returns:
            None
    """
    urls = get_bill_urls(directory_url)
    
    for url in urls:
        download_bill(url, year_dir)

def download_all_directories(min_year, max_year, raw_dir): 
    """
        Downloads all files within the given interval (inclusive). Stores data
        in the given directory path. 

        Arguments: 
            (int) min_year: minimum year of interval
            (int) max_year: maximum year of interval
            (Path) raw_dir: path object representing directory to store raw data files.
        Returns: 
            None
    """
    directory_urls = []

    for i in range(min_year, max_year - 1):
        year_id = str(i) + str(i + 1)
        year_dir = raw_dir / year_id

        if not year_dir.exists():
            os.mkdir(year_dir)

        url = "https://leginfo.legislature.ca.gov/faces/billSearchClient.xhtml?session_year=" + year_id + "&house=Both&author=All&lawCode=All"
        directory_urls.append((url, year_dir , i)) # Packaging arguments as tuples to ensure thread safety

    with concurrent.futures.ThreadPoolExecutor(max_workers= 10) as executor:
        executor.map(lambda f: download_directory(*f), directory_urls)
    

def main():
    min_year = 1999
    max_year = 2022
    
    if not (Path.cwd() / 'data').exists():
        os.mkdir(Path() / 'data')

    if not (Path.cwd() / 'data' / "raw").exists():
        os.mkdir(Path() / 'data' / "raw")

    raw_dir = Path() / "data" / "raw"

    download_all_directories(min_year, max_year, raw_dir)

if __name__ == "__main__":
    main()
    print("Finished Scraping! :D")