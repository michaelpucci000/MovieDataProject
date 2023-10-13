import gzip
import shutil
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd

# Step 1: Get the list of files from the URL
base_url = 'https://datasets.imdbws.com/'
response = requests.get(base_url)
soup = BeautifulSoup(response.text, 'html.parser')
files_to_download = [urljoin(base_url, link.get('href')) for link in soup.find_all('a') if link.get('href').endswith('.gz')]

# Step 2: Create a function to download and unzip files
def download_and_unzip(url):
    file_name = url.split('/')[-1]
    csv_file_name = file_name.replace('.gz', '.tsv')

    response = requests.get(url)
    with open(file_name, 'wb') as file:
        file.write(response.content)

    with gzip.open(file_name, 'rb') as f_in:
        with open(csv_file_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return csv_file_name

# Step 3: Create a database and tables to store the data
conn = sqlite3.connect('imdb_movies.db')

# Step 4: Download, unzip and load each file into a separate table in the database
for file_url in files_to_download:
    csv_file_name = download_and_unzip(file_url)
    
    # Get the table name from the file name
    table_name = csv_file_name.replace('.tsv', '')
    
    # Read the data into a pandas DataFrame
    data = pd.read_csv(csv_file_name, delimiter='\t', encoding='utf-8', on_bad_lines='warn')
    
    # Insert the data into a new table in the database
    data.to_sql(table_name, conn, if_exists='replace', index=False)

# Close the database connection
conn.close()