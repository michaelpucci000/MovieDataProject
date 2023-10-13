from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re
import time
import pyodbc

# Define the slow_scroll function
def slow_scroll(browser, scroll_pause_time=0.5, increment=200):
    last_height = browser.execute_script("return document.body.scrollHeight")
    while True:
        for i in range(0, last_height, increment):
            browser.execute_script(f"window.scrollTo(0, {i});")
            time.sleep(scroll_pause_time)
        
        new_height = browser.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

# This function scrapes movie links from the current page
def scrape_current_page(browser):
    slow_scroll(browser)
    soup = BeautifulSoup(browser.page_source, "html.parser")
    return [base_url + link['href'] for link in soup.select('a.frame')]

def create_table():
    # Create a connection string
    connection_string = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=DESKTOP-PICNRMM\SQLEXPRESS;'
        r'DATABASE=LetterboxdVImdb;'
        r'UID=mikepucci;'
        r'PWD=6604Heidelberg!'
    )

    # Connect to SQL Server
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Create table query
    create_query = """
        CREATE TABLE LBIMDbTop250 (
            id INT PRIMARY KEY IDENTITY(1,1),
            title NVARCHAR(255),
            year NVARCHAR(50),
            director NVARCHAR(255),
            cast NVARCHAR(MAX),
            studio NVARCHAR(255),
            country NVARCHAR(100),
            language NVARCHAR(100),
            genre NVARCHAR(255),
            letterboxdRating FLOAT,
            letterboxdWatched INT,
            letterboxdRank INT,
            source NVARCHAR(255),
            masterMovieId NVARCHAR(255)
        )
    """
    cursor.execute(create_query)

    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

def insert_into_sql(data):
    # Create a connection string
    connection_string = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=DESKTOP-PICNRMM\SQLEXPRESS;'
        r'DATABASE=LetterboxdVImdb;'
        r'UID=mikepucci;'
        r'PWD=6604Heidelberg!'
    )
    
    # Connect to SQL Server
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    # Assuming you have a table named 'LBIMDbTop250' with columns that match the dictionary keys.
    for movie in data:
        query = """INSERT INTO LBIMDbTop250(title, year, director, cast, studio, country, language, genre, letterboxdRating, letterboxdWatched, letterboxdRank, source, masterMovieId)
                   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        cursor.execute(query, (movie['title'], movie['year'], movie['director'], 
                                ','.join(movie['cast']), movie['studio'], movie['country'], movie['language'], 
                                ','.join(movie['genre']), movie['letterboxdRating'], movie['letterboxdWatched'], 
                                movie['letterboxdRank'], movie['source'], movie['masterMovieId']))
    
    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

base_url = 'https://letterboxd.com'
list_url = f'{base_url}/dave/list/imdb-top-250/'

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--ignore-certificate-errors")
browser = webdriver.Chrome()

film_links = []
total_pages = 3

# Loop through all the pages
for page_num in range(1, total_pages + 1):
    if page_num == 1:
        browser.get(list_url)
    else:
        # If not the first page, navigate to the next one
        browser.get(list_url + 'page/' + str(page_num) + '/')
    film_links.extend(scrape_current_page(browser))
    time.sleep(2)

print(f"Total film links found: {len(film_links)}")

data = []

for url in film_links:
    browser.get(url)
    soup = BeautifulSoup(browser.page_source, "html.parser")
    
    # Extracting individual fields
    title = soup.select_one('h1.headline-1').text.strip()

    year = soup.select_one('small.number a').text.strip()

    director_element = soup.select_one('span.prettify')
    director = director_element.text.strip() if director_element else None

    cast_members = soup.select('p > a.text-slug.tooltip')
    cast = [actor.text.strip() for actor in cast_members]

    studio = soup.select_one('p > a[href^="/studio/"]').text.strip() if soup.select_one('p > a[href^="/studio/"]') else None

    country = soup.select_one('p > a[href^="/films/country/"]').text.strip()

    language = soup.select_one('p > a[href^="/films/language/"]').text.strip()

    genres_elements = soup.select('p > a[href^="/films/genre/"]')
    genres = [genre.text.strip() for genre in genres_elements]

    rating_tag = soup.select_one('a.tooltip.display-rating')
    if rating_tag:
        rating_text = rating_tag.get('data-original-title')
        rating_match = re.search(r"(\d+\.\d+)", rating_text)
        if rating_match:
            rating = float(rating_match.group(1))
    else:
        rating = None

    watched_by_tag = soup.select_one('a[href^="/film/"][data-original-title^="Watched by"]')
    if watched_by_tag:
        watched_by_text = watched_by_tag.get('data-original-title')
        watched_by = int(re.sub('[^0-9]', '', watched_by_text))  # Remove all non-digit characters
    else:
        watched_by = None

    top250_element = soup.select_one('li.filmstat-top250 a')
    if top250_element:
        no_in_letterboxd_top_250 = int(top250_element.text.strip())
    else:
        no_in_letterboxd_top_250 = None

    imdb_link_element = soup.select_one('a[href^="http://www.imdb.com/title/"]')
    if imdb_link_element:
        href = imdb_link_element['href']
        match = re.search(r'tt\d+', href)
        if match:
            master_movie_id = match.group()
        else:
            master_movie_id = None
    else:
        master_movie_id = None

    data.append({
        'title': title,
        'year': year,
        'director': director,
        'cast': cast,
        'studio': studio,
        'country': country,
        'language': language,
        'genre': genres,
        'letterboxdRating': rating,
        'letterboxdWatched': watched_by,
        'letterboxdRank': no_in_letterboxd_top_250,
        'source': 'Letterboxd',
        'masterMovieId': master_movie_id
    })

    print(data[-1])  # Print the most recent entry
    time.sleep(2)

# Close the browser
browser.quit()

create_table()

insert_into_sql(data)
