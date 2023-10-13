import pyodbc
import requests
from bs4 import BeautifulSoup
from time import sleep
import re

server = 'DESKTOP-PICNRMM\SQLEXPRESS'
database = 'LetterboxdVImdb'
username = 'mikepucci'
password = '6604Heidelberg!'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
cursor = cnxn.cursor()

# Creating the table
create_table_query = '''
CREATE TABLE IMDbLetterboxdTop250 (
    id INT PRIMARY KEY IDENTITY(1,1),
    title NVARCHAR(255),
    year NVARCHAR(50),
    director NVARCHAR(255),
    cast NVARCHAR(MAX),
    studio NVARCHAR(255),
    country NVARCHAR(100),
    language NVARCHAR(100),
    genre NVARCHAR(255),
    imdbRating FLOAT,
    imdbWatched INT,
    imdbRank INT,
    source NVARCHAR(255),
    masterMovieId NVARCHAR(255)
)
'''
cursor.execute(create_table_query)
cnxn.commit()

BASE_URL = "https://www.imdb.com"
TOP_URL_TEMPLATE = BASE_URL + "/list/ls521695716/?page={page_number}"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'}

# Assuming 3 pages for the top 250 movies
movie_links = []

for page_number in range(1, 4):  # 3 pages
    response = requests.get(TOP_URL_TEMPLATE.format(page_number=page_number), headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extend the movie_links list with links found on this page
    movie_links.extend([a['href'] for a in soup.select('h3.lister-item-header a[href^="/title/"]')])

print(f"Found {len(movie_links)} movie URLs.")

for link in movie_links:
    data = {}
    movie_url = BASE_URL + link
    movie_response = requests.get(movie_url, headers=headers)
    movie_soup = BeautifulSoup(movie_response.content, 'html.parser')

    data['title'] = movie_soup.select_one('span.sc-afe43def-1.fDTGTb').text.strip()
    
    year_element = movie_soup.select_one('a.ipc-link--baseAlt[href^="/title/"][href$="/releaseinfo?ref_=tt_ov_rdat"]')
    if year_element:
        data['year'] = year_element.text.strip()
    else:
        data['year'] = None

    data['director'] = movie_soup.select_one('a.ipc-metadata-list-item__list-content-item.ipc-metadata-list-item__list-content-item--link[href^="/name/"]').text.strip()

    cast_members = movie_soup.select('a.sc-bfec09a1-1.fUguci')
    data['cast'] = [cast.text.strip() for cast in cast_members]

    data['studio'] = movie_soup.select_one('a.ipc-metadata-list-item__list-content-item.ipc-metadata-list-item__list-content-item--link[href^="/company/"]').text.strip()

    data['country'] = movie_soup.select_one('a.ipc-metadata-list-item__list-content-item.ipc-metadata-list-item__list-content-item--link[href^="/search/title/?country_of_origin="]').text.strip()

    language_element = movie_soup.select_one('a.ipc-metadata-list-item__list-content-item.ipc-metadata-list-item__list-content-item--link[href^="/search/title?title_type=feature&primary_language="]')
    if language_element:
        data['language'] = language_element.text.strip()
    else:
        data['language'] = "None"

    genres_elements = movie_soup.select('a.ipc-chip')
    data['genre'] = [genre.span.text.strip() for genre in genres_elements]

    data['imdbRating'] = movie_soup.select_one('span.sc-bde20123-1.iZlgcd').text.strip()

    watched_by_text = movie_soup.select_one('div.sc-bde20123-3.bjjENQ').text.strip()
    if 'M' in watched_by_text:
        data['imdbWatched'] = int(float(watched_by_text.replace('M', '')) * 1e6)
    elif 'K' in watched_by_text:
        data['imdbWatched'] = int(float(watched_by_text.replace('K', '')) * 1e3)
    else:
        data['imdbWatched'] = int(watched_by_text)  # for the rare case where there's no 'M' or 'K'

    rank_element = movie_soup.select_one('a.top-rated-link')
    if rank_element:
        rank_text = rank_element.text.strip()
        rank = rank_text.split("#")[1]
        data['imdbRank'] = int(rank)
    else:
        data['imdbRank'] = None

    data['source'] = 'IMDb'

    match = re.search(r'tt\d+', link)
    if match:
        data['masterMovieId'] = match.group()
    else:
        data['masterMovieId'] = None

    print("-" * 40)  # prints a separator
    for key, value in data.items():
        print(f"{key}: {value}")

    # Inserting into the database
    insert_query = '''INSERT INTO IMDbLetterboxdTop250(title, year, director, cast, studio, country, language, genre, imdbRating, imdbWatched, imdbRank, source, masterMovieId)
                   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    cursor.execute(insert_query, (data['title'], data['year'], data['director'], 
                            ','.join(data['cast']), data['studio'], data['country'], data['language'], 
                            ','.join(data['genre']), data['imdbRating'], data['imdbWatched'], 
                            data['imdbRank'], data['source'], data['masterMovieId']))
    
    cnxn.commit()

    sleep(2)

cursor.close()
cnxn.close()