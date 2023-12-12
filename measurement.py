import sqlite3

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


def write_statistics_to_file(stats, filename):
    """
    Write the calculated statistics to a text file.

    Input:
        stats (dict): The calculated statistics to write.
        filename (str): The filename to which the statistics will be written.

    Output:
        None: The function writes to a file but does not return a value.

    Description:
        Writes the statistics (such as the number of unique songs per artist) for each year to a text file.
    """
    with open(filename, 'w') as f:
        for year, data in stats.items():
            f.write(f"Year: {year}\n")
            for artist, count in data:
                f.write(f"{artist}: {count} songs\n")
            f.write("\n")

def main():
    """
    Main function to execute the statistics calculation and writing process.

    Input:
        None

    Output:
        None

    Description:
        Orchestrates the process of calculating statistics from the SQLite database and writing these statistics to a text file.
    """
    db_path = 'billboard_top_songs.db'
    stats = calculate_statistics(db_path)
    write_statistics_to_file(stats, 'song_statistics.txt')

if __name__ == "__main__":
    main()
