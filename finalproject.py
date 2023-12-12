import requests
from bs4 import BeautifulSoup
import sqlite3
import os

def get_last_processed_index(db_path):
    """
    Retrieve the last processed index from the State table in the database.

    Input:
        db_path (str): The file path to the SQLite database.

    Output:
        int: The last processed index from the database or 0 if no records exist.

    Description:
        Connects to the SQLite database, checks for the existence of the State table,
        and retrieves the last processed index. It's used to determine where to resume data scraping.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS State (LastIndex INTEGER)")
    last_index = c.execute("SELECT LastIndex FROM State ORDER BY ROWID DESC LIMIT 1").fetchone()
    conn.close()
    return last_index[0] if last_index else 0

def set_last_processed_index(db_path, index):
    """
    Set the last processed index in the State table of the database.

    Input:
        db_path (str): The file path to the SQLite database.
        index (int): The index to be set as the last processed index in the database.

    Output:
        None

    Description:
        Updates the State table in the database with the provided index, marking it as the last processed index.
        Essential for tracking progress and ensuring data isn't reprocessed unnecessarily.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("INSERT INTO State (LastIndex) VALUES (?)", (index,))
    conn.commit()
    conn.close()

def create_artist_id_table(db_path):
    """
    Create the ArtistIDs table in the database if it doesn't exist.

    Input:
        db_path (str): The file path to the SQLite database.

    Output:
        None: The function creates a table but does not return a value.

    Description:
        Ensures the existence of the ArtistIDs table in the database, which stores artist names and their associated IDs.
        Crucial for maintaining a normalized database structure.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS ArtistIDs (
            ArtistID INTEGER PRIMARY KEY AUTOINCREMENT,
            ArtistName TEXT
        )
    ''')
    conn.commit()
    conn.close()

def get_or_create_artist_id(db_path, artist_name):
    """
    Get or create an artist ID for a given artist name.

    Input:
        db_path (str): The file path to the SQLite database.
        artist_name (str): The name of the artist.

    Output:
        int: The artist ID.

    Description:
        Retrieves an artist's ID from the database if it exists; otherwise, creates a new record and returns the new ID.
        Helps to avoid duplicate entries for the same artist.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT ArtistID FROM ArtistIDs WHERE ArtistName=?", (artist_name,))
    artist_id = c.fetchone()
    if artist_id is None:
        c.execute("INSERT INTO ArtistIDs (ArtistName) VALUES (?)", (artist_name,))
        conn.commit()
        c.execute("SELECT ArtistID FROM ArtistIDs WHERE ArtistName=?", (artist_name,))
        artist_id = c.fetchone()
    conn.close()
    return artist_id[0]

def scrape_songs(url, db_path, start_index=0):
    """
    Scrape song data from a URL and store it in the database.

    Input:
        url (str): The URL to scrape song data from.
        db_path (str): The file path to the SQLite database.
        start_index (int): The starting index for scraping.

    Output:
        list: A list of tuples containing song data.

    Description:
        Scrapes song data from the provided URL, starting from the start_index.
        Collects information about song rankings, names, and artists, then associates them with artist IDs.
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    songs = []
    entries = soup.find_all('h3', {'class': 'c-title'})
    for i, entry in enumerate(entries[start_index:], start=start_index):
        if i >= start_index + 25:
            break
        song_name = entry.get_text(strip=True)
        artist_element = entry.find_next('span', {'class': 'c-label'})
        rank_element = entry.find_previous('span', {'class': 'c-label'})
        if artist_element is not None and rank_element is not None:
            artist_name = artist_element.get_text(strip=True)
            rank = rank_element.get_text(strip=True)
            artist_id = get_or_create_artist_id(db_path, artist_name)
            songs.append((rank, artist_name, song_name, artist_id))
    return songs

def main(url_2020, url_2021):
    """
    Main function to orchestrate the scraping and storage of song data.

    Input:
        url_2020 (str): URL for 2020 song data.
        url_2021 (str): URL for 2021 song data.

    Output:
        None

    Description:
        Coordinates the creation of database tables, retrieval of the last processed index,
        scraping of song data for 2020 and 2021, insertion of data into the database.
    """
    db_path = 'billboard_top_songs.db'
    create_artist_id_table(db_path)
    start_index = get_last_processed_index(db_path)
    songs_2020 = scrape_songs(url_2020, db_path, start_index)
    songs_2021 = scrape_songs(url_2021, db_path, start_index)

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS BillboardTopSongs2020 (
            Rank INTEGER,
            ArtistName TEXT,
            SongName TEXT,
            ArtistID INTEGER
        )
    ''')
    c.executemany('INSERT INTO BillboardTopSongs2020 (Rank, ArtistName, SongName, ArtistID) VALUES (?, ?, ?, ?)', songs_2020)
    c.execute('''
        CREATE TABLE IF NOT EXISTS BillboardTopSongs2021 (
            Rank INTEGER,
            ArtistName TEXT,
            SongName TEXT,
            ArtistID INTEGER
        )
    ''')
    c.executemany('INSERT INTO BillboardTopSongs2021 (Rank, ArtistName, SongName, ArtistID) VALUES (?, ?, ?, ?)', songs_2021)
    conn.commit()
    conn.close()

    set_last_processed_index(db_path, start_index + 25)

if __name__ == "__main__":
    url_2020 = "https://www.billboard.com/charts/year-end/2020/hot-100-songs/"
    url_2021 = "https://www.billboard.com/charts/year-end/2021/hot-100-songs/"
    main(url_2020, url_2021)
