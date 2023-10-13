import os
import requests
import pyodbc
from urllib.parse import urljoin
from gzip import GzipFile
from io import BytesIO
from bs4 import BeautifulSoup

# Database connection parameters
server = 'DESKTOP-PICNRMM\SQLEXPRESS'
database = 'Imdb'  
username = 'mikepucci'  
password = '6604Heidelberg!'  
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()

# Define base URL
base_url = "https://datasets.imdbws.com/"

# Function to get list of file names from the website
def get_file_names(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = [link.get('href') for link in soup.find_all('a') if link.get('href', '').endswith('.gz')]
    return [os.path.basename(link) for link in links]

# Function to create necessary tables dynamically based on column names in file
def create_table(file_path, table_name):
    with open(file_path, 'r', encoding='utf-8') as file:
        columns = file.readline().strip().split('\t')
        columns = [col.replace('-', '_') for col in columns]
        cols_with_types = [f"[{col}] VARCHAR(MAX)" for col in columns]
        cols_as_str = ', '.join(cols_with_types)
        create_query = f"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U') CREATE TABLE {database}.dbo.{table_name} ({cols_as_str})"
        cursor.execute(create_query)
        cnxn.commit()

# Function to download, unzip and load data into SQL Server
def process_file(url, file_name):
    response = requests.get(url)
    if response.status_code == 200:
        # Unzipping the file
        with GzipFile(fileobj=BytesIO(response.content)) as f:
            # Saving the unzipped file as csv
            with open(file_name.replace(".gz", ""), "wb") as outfile:
                outfile.write(f.read())

        # Create the necessary table
        table_name = file_name.replace(".tsv.gz", "").replace(".", "_")
        csv_file_path = os.path.abspath(file_name.replace(".gz", ""))
        create_table(csv_file_path, table_name)

        # Preparing the BULK INSERT query
        query = f"""
        BULK INSERT {database}.dbo.{table_name}
        FROM '{csv_file_path}'
        WITH (
            FIELDTERMINATOR = '\t',
            ROWTERMINATOR = '0x0a',
            FIRSTROW = 2,
            BATCHSIZE = 100000,
            TABLOCK
        );
        """

        # Executing the BULK INSERT query
        try:
            cursor.execute(query)
            cnxn.commit()
        except Exception as e:
            print(f"An error occurred while inserting data: {e}")
            cnxn.rollback()

# Get file names from the website and process them
file_names = get_file_names(base_url)
for file_name in file_names:
    url = urljoin(base_url, file_name)
    process_file(url, file_name)

# Close the database connection
cursor.close()
cnxn.close()