import pandas as pd
import requests
import os
import pyodbc
import gzip
from sqlalchemy import create_engine

# 1. Open the IMDB datasets webpage and fetch required gzipped files
IMDB_URL = "https://datasets.imdbws.com/"
FILES = [
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
    "name.basics.tsv.gz"
]

for file in FILES:
    response = requests.get(IMDB_URL + file, stream=True)
    with open(file, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

# 2. Connect to the SQL Server
connection_string = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=DESKTOP-PICNRMM\SQLEXPRESS;'
    r'DATABASE=Imdb2;'
    r'UID=mikepucci;'
    r'PWD=6604Heidelberg!'
)
conn = pyodbc.connect(connection_string, autocommit=True)
cursor = conn.cursor()

# Create an SQLAlchemy engine for pandas
engine = create_engine("mssql+pyodbc://mikepucci:6604Heidelberg!@DESKTOP-PICNRMM\SQLEXPRESS/Imdb2?driver=ODBC+Driver+17+for+SQL+Server")

# Schema information
SCHEMA = {
    "title_akas": {
        "columns": {
            "titleId": "VARCHAR(20)",
            "ordering": "INT",
            "title": "VARCHAR(MAX)",
            "region": "VARCHAR(10)",
            "language": "VARCHAR(10)",
            "types": "VARCHAR(MAX)",
            "attributes": "VARCHAR(MAX)",
            "isOriginalTitle": "BIT"
        },
        "primary_key": ["titleId"]
    },
    "title_basics": {
        "columns": {
            "tconst": "VARCHAR(20)",
            "titleType": "VARCHAR(50)",
            "primaryTitle": "VARCHAR(MAX)",
            "originalTitle": "VARCHAR(MAX)",
            "isAdult": "BIT",
            "startYear": "VARCHAR(5)",
            "endYear": "VARCHAR(5)",
            "runtimeMinutes": "INT",
            "genres": "VARCHAR(MAX)"
        },
        "primary_key": ["tconst"]
    },
    "title_crew": {
        "columns": {
            "tconst": "VARCHAR(20)",
            "directors": "VARCHAR(MAX)",
            "writers": "VARCHAR(MAX)"
        },
        "primary_key": ["tconst"]
    },
    "title_episode": {
        "columns": {
            "tconst": "VARCHAR(20)",
            "parentTconst": "VARCHAR(20)",
            "seasonNumber": "INT",
            "episodeNumber": "INT"
        },
        "primary_key": ["tconst"]
    },
    "title_principals": {
        "columns": {
            "tconst": "VARCHAR(20)",
            "ordering": "INT",
            "nconst": "VARCHAR(20)",
            "category": "VARCHAR(MAX)",
            "job": "VARCHAR(MAX)",
            "characters": "VARCHAR(MAX)"
        },
        "primary_key": ["tconst"]
    },
    "title_ratings": {
        "columns": {
            "tconst": "VARCHAR(20)",
            "averageRating": "FLOAT",
            "numVotes": "INT"
        },
        "primary_key": ["tconst"]
    },
    "name_basics": {
        "columns": {
            "nconst": "VARCHAR(20)",
            "primaryName": "VARCHAR(MAX)",
            "birthYear": "VARCHAR(5)",
            "deathYear": "VARCHAR(5)",
            "primaryProfession": "VARCHAR(MAX)",
            "knownForTitles": "VARCHAR(MAX)"
        },
        "primary_key": ["nconst"]
    }
}

def create_table_if_not_exists(table_name, schema):
    columns = ', '.join([f"{col} {dtype}" for col, dtype in schema["columns"].items()])
    primary_key = ', '.join(schema["primary_key"])
    
    # Check if the table exists
    cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    if not cursor.fetchone():
        # If the table doesn't exist, create it
        create_table_sql = f"""CREATE TABLE dbo.{table_name} ({columns}, PRIMARY KEY ({primary_key}))"""
        print(create_table_sql)
        cursor.execute("USE Imdb2")
        cursor.execute(create_table_sql)

# 3. For each file, decompress and load data into the SQL Server
for file in FILES:
    print(f"Processing file: {file}")
    # Decompress
    with gzip.open(file, 'rt', encoding='utf-8') as f:
        # Use pandas for efficient data loading
        chunk_size = 50000  # Number of rows to process at a time

        # Construct table name by removing file extension
        table_name = '_'.join(file.split('.')[:2])
        print(f"Extracted table name: {table_name}")

        # Create table if it doesn't exist
        create_table_if_not_exists(table_name, SCHEMA[table_name])

        for chunk in pd.read_csv(f, sep='\t', chunksize=chunk_size, low_memory=False, na_values=["\\N"]):
            # Try inserting data
            try:
                chunk.to_sql(table_name, engine, if_exists='append', index=False, method='multi', chunksize=chunk_size, schema='dbo')
            except Exception as e:
                print(f"Error while inserting data into {table_name}: {e}")

# 4. Clean up by removing the gzipped files
for file in FILES:
    os.remove(file)

print("Data loading complete!")