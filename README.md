# IMDb vs. Letterboxd Top 250 Movies Comparison

IMDb and Letterboxd are the two largest social catergorizing sites for movies. This project aims to compare the top 250 movies list from both IMDb and Letterboxd websites. The primary goal is to explore the differences in movie acclaim between the user bases of these two platforms.

## Motivation

I've was curious about how different communities value and rank movies. I am an avid film buff and Letterboxd user By comparing the top movies from both IMDb and Letterboxd, I hoped to gain insights into the distinct preferences of each site's audience.

## Data Collection Process

### Initial Approach

Initially, I attempted to utilize the `.gz` files provided by IMDb. However, these files proved to be too vast and disorganized for effective analysis.

### Web Scraping

Given the challenges with the initial data, I built web scrapers for both IMDb and Letterboxd. These scrapers search for specific tags on each site to extract the desired metadata, including:

- Title
- Year
- Director
- Cast
- Genre
- Country
- Language
- Rating
- Number of times watched

Once the data is scraped, it's directly loaded into an SQL server for further processing.

## Data Processing

On the SQL server:

- **Data Cleaning**: I created queries to organize and clean the scraped data into a master table.
- **Analysis**: Using SQL functions like `GROUP BY` and `COALESCE`, I aggregated the data based on various dimensions like genre and year.

## Visualization

After processing the data, I imported it into PowerBI to design a comprehensive dashboard that showcases the insights derived from the comparison.
