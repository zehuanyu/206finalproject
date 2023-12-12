import sqlite3
import matplotlib.pyplot as plt

def calculate_statistics(db_path):
    """
    Calculate statistics from the database tables using JOIN.

    Input:
        db_path (str): The file path to the SQLite database.

    Output:
        dict: A dictionary with calculated statistics for each year.

    Description:
        Calculates statistics such as the number of unique songs per artist for each year.
        This is done by JOINING the BillboardTopSongs tables with the ArtistIDs table.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    stats = {}
    for year in [2020, 2021]:
        table_name = f'BillboardTopSongs{year}'
        c.execute(f'''
            SELECT ArtistIDs.ArtistName, COUNT(*) as SongCount
            FROM {table_name}
            JOIN ArtistIDs ON {table_name}.ArtistID = ArtistIDs.ArtistID
            GROUP BY {table_name}.ArtistID
        ''')
        artist_song_counts = c.fetchall()
        stats[year] = artist_song_counts

    conn.close()
    return stats

def plot_artist_song_counts(stats):
    for year, artist_counts in stats.items():
        artists, counts = zip(*artist_counts)
        num_artists = len(artists)
        
        # Increase the figure size and adjust the bar width
        plt.figure(figsize=(10, num_artists * 0.5))  # Adjust the multiplier as needed
        plt.barh(artists, counts, height=0.5)  # Adjust the height as needed
        
        plt.xlabel('Number of Songs')
        plt.ylabel('Artist')
        plt.title(f'Number of Songs per Artist - Year {year}')
        plt.tight_layout()
        plt.savefig(f'artist_song_counts_{year}.png')
        plt.show()

def main():
    db_path = 'billboard_top_songs.db'
    stats = calculate_statistics(db_path)
    plot_artist_song_counts(stats)

if __name__ == "__main__":
    main()
