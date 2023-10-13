import gzip
import shutil
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import sqlite3
import re
import logging

# Setting up logging to file
logging.basicConfig(filename='sql_commands.log', level=logging.DEBUG)

# Step 1: Get the list of files from the URL
base_url = 'https://datasets.imdbws.com/'
response = requests.get(base_url)
soup = BeautifulSoup(response.text, 'html.parser')
files_to_download = [urljoin(base_url, link.get('href')) for link in soup.find_all('a') if link.get('href').endswith('.gz')]

# Step 2: Create a function to download and unzip files
def download_and_unzip(url):
    file_name = url.split('/')[-1]
    tsv_file_name = file_name.replace('.gz', '.tsv')
    
    response = requests.get(url)
    with open(file_name, 'wb') as file:
        file.write(response.content)
    
    with gzip.open(file_name, 'rb') as f_in:
        with open(tsv_file_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    return tsv_file_name

# Step 3: Create a database and tables to store the data
conn = sqlite3.connect('imdb_movies2.db')

# Step 4: Download, unzip and load each file into a separate table in the database
for file_url in files_to_download:
    tsv_file_name = download_and_unzip(file_url)
    
    # Get the table name from the file name
    table_name = re.sub(r'\W+', '_', tsv_file_name.replace('.tsv', '')).lower()
    
    # Get the column names from the first line of the file
    with open(tsv_file_name, 'r', encoding='utf-8') as file:
        columns = file.readline().strip().split('\t')
    
    # Create a table with the appropriate column names
    create_table_command = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} TEXT' for col in columns])})"
    logging.debug(create_table_command)
    conn.execute(create_table_command)
    
    # Read the data line by line and insert it into the database in bulk
    with open(tsv_file_name, 'r', encoding='utf-8') as file:
        # Skip the header line
        next(file)
        
        # Prepare for bulk insert
        cursor = conn.cursor()
        chunk_size = 1000
        chunk = []
        
        for line in file:
            values = line.strip().split('\t')
            chunk.append(values)
            
            if len(chunk) >= chunk_size:
                insert_command = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in columns])})"
                logging.debug(insert_command)
                cursor.executemany(insert_command, chunk)
                chunk = []
        
        # Insert any remaining rows
        if chunk:
            insert_command = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in columns])})"
            logging.debug(insert_command)
            cursor.executemany(insert_command, chunk)
    
    # Commit the transaction and close the cursor
    conn.commit()
    cursor.close()

# Close the database connection
conn.close()