from dotenv import load_dotenv
import os
import base64
import requests
import json
import sqlite3
import pprint
import matplotlib.pyplot as plt
import random

######################## Spotify Authentification ########################
load_dotenv()

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")


def get_token():
    if not client_id or not client_secret:
        raise ValueError("Client ID or Client Secret is missing.")

    auth_string = f"{client_id}:{client_secret}"
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        json_result = response.json()
        token = json_result.get("access_token")
        if not token:
            print("Failed to retrieve access token:", json_result)
        return token
    except requests.exceptions.RequestException as e:
        print(f"HTTP Request failed: {e}")

def get_auth_header(token):
    return {"Authorization": "Bearer " + token}

token = get_token()

######################## Retrieving Data From API ########################
def get_playlist(playlist_id, token):
    try:
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        headers = get_auth_header(token)
        response = requests.get(url, headers=headers)
        return response.json()
    except:
        return "Not Found"

def get_track_info(playlist_data):
    track_list = []
    for item in playlist_data.get('items', []):
        track = item.get('track', {})
        track_name = track.get('name', 'No Track Name')
        for artist in track.get('artists', []):
            artist_name = artist.get('name', 'No Artist Name')
            track_list.append({"Artist": artist_name, "Track": track_name})
    return track_list

data_2020_link = "2fmTTbBkXi8pewbUvG3CeZ"
playlist_2020 = get_playlist(data_2020_link, token)

data_2021_link = '5GhQiRkGuqzpWZSE7OU4Se'
playlist_2021 = get_playlist(data_2021_link, token)

with open('2020.json', 'w') as file:
    json.dump(playlist_2020, file, indent=4)

with open('2021.json', 'w') as file:
    json.dump(playlist_2021, file, indent=4)

data2020 = get_track_info(playlist_2020)
data2021 = get_track_info(playlist_2021)

######################## DATABASE ########################
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def create_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS Artists (id INTEGER PRIMARY KEY AUTOINCREMENT, Artist_name TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS Tracks_2020 (Rank INTEGER PRIMARY KEY AUTOINCREMENT, Artist_name Text, Song_name TEXT,id, FOREIGN KEY (id) REFERENCES Artists(id))')
    cur.execute('CREATE TABLE IF NOT EXISTS Tracks_2021 (Rank INTEGER PRIMARY KEY AUTOINCREMENT, Artist_name Text, Song_name TEXT,id, FOREIGN KEY (id) REFERENCES Artists(id))')

    conn.commit()

def add_artists(data, cur, conn):
    info = get_track_info(data)
    count = 0 

    for i in info:
        if count < 12:
            Artist = i['Artist']
            cur.execute('SELECT id FROM Artists WHERE Artist_name = ?', (Artist,))
            if not cur.fetchone():
                cur.execute('INSERT OR IGNORE INTO Artists (Artist_name) VALUES (?)', (Artist,))
                conn.commit()
                count += 1
    
def add_track(name, data, cur, conn):
    info = get_track_info(data)

    cur.execute(f'SELECT MAX(Rank) FROM {name}')
    max = cur.fetchone()
    if max[0] is not None:
        max_rank = max[0]
    else:
        max_rank = 0

    for i in info[max_rank:max_rank + 25]:
        Artist = i['Artist']
        Song = i['Track']

        cur.execute('SELECT id FROM Artists WHERE Artist_name = ?', (Artist,))
        artist_id = cur.fetchone()

        if artist_id:
            artist_id = artist_id[0]
            cur.execute(f'INSERT OR IGNORE INTO {name} (Artist_name, Song_name, id) VALUES (?,?,?)', (Artist, Song, artist_id))
            conn.commit()

def artists_in_both_years(cur):
    cur.execute("SELECT DISTINCT Artists.Artist_name FROM Artists JOIN Tracks_2020 ON Artists.id = Tracks_2020.id JOIN Tracks_2021 ON Artists.id = Tracks_2021.id")
    return cur.fetchall()

def unique_artists(cur, year):
    if year == 2020:
        cur.execute("SELECT DISTINCT Artists.Artist_name FROM Artists JOIN Tracks_2020 ON Artists.id = Tracks_2020.id WHERE Artists.id NOT IN (SELECT id FROM Tracks_2021)")
    elif year == 2021:
        cur.execute("SELECT DISTINCT Artists.Artist_name FROM Artists JOIN Tracks_2021 ON Artists.id = Tracks_2021.id WHERE Artists.id NOT IN (SELECT id FROM Tracks_2020)")
    return cur.fetchall()

def write_artists(filename, artists):
    with open(filename, 'w') as file:
        for artist in artists:
            file.write(artist[0] + "\n")


cur, conn = setUpDatabase('DataBase')
create_table(cur, conn)
add_artists(playlist_2021, cur,conn)
add_artists(playlist_2020, cur,conn)

add_track('Tracks_2021', playlist_2021, cur,conn)
add_track('Tracks_2020', playlist_2020, cur,conn)

artists_in_both = artists_in_both_years(cur)
write_artists('artists_in_both_years.txt', artists_in_both)

######################## VISUALIZE ########################

#### 1 ####
artists_in_both_count = len(artists_in_both)
artists_2020 = unique_artists(cur, 2020)
artists_2021 = unique_artists(cur, 2021)
artists_2020_count = len(artists_2020)
artists_2021_count = len(artists_2021)

sizes = [artists_in_both_count, artists_2020_count, artists_2021_count]
labels = ['Artists in Both Years', 'Artists Only in 2020', 'Artists Only in 2021']
colors = ['skyblue', 'coral', 'lightgreen']

fig1, ax1 = plt.subplots()
ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=140, pctdistance=0.85)
centre_circle = plt.Circle((0, 0), 0.70, fc='Black')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)

plt.text(0, 0, 'SPOTIFY', ha='center', va='center', fontsize=40, color='lightgreen')
plt.title(' Pie Chart of Artists Across 2020 and 2021')
plt.legend(loc='lower left', bbox_to_anchor=(-0.3, 0))
plt.tight_layout()
plt.show()

#### 2 ####
fig2, ax2 = plt.subplots()
colors=['skyblue', 'coral']
years = ['2020', '2021']
artist_counts = [artists_2020_count, artists_2021_count]

ax2.bar(years, artist_counts, width=0.5, color=colors)

plt.xlabel('Year', fontsize=12)
plt.ylabel('Number of Unique Artists', fontsize=12)
plt.title('Unique Artists in Spotify Playlists: 2020 vs 2021', fontsize=14)
plt.xticks(years, fontsize=10, rotation=45)
plt.yticks(fontsize=10)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()