import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def get_files(filepath):
    """
    Extract JSON files from a directory
    
    Parameters: 
        filepath (string): parent folder path
        
    Returns:
        list of individual file paths       
    """
    
    # Find all JSON files in directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
            
    num_files = len(all_files)
    
    # Print number of files found in directory
    print('\n{} files found in {}\n'.format(num_files, filepath))
    
    return all_files


def json_to_df(files):
    """
    Store JSON files as rows in a DataFrame
    
    Parameters:
        files (list): list of file path strings
        
    Returns:
        DataFrame populated from JSON files as rows
    """
    
    # Initialize DataFrame
    df = pd.DataFrame()
    
    # Add files as rows to DataFrame
    for i in range(len(files)):
        file = pd.read_json(files[i], lines = True)
        
        # Concatenate new file as row
        df = pd.concat([df, file], axis = 0, ignore_index = True)
    
    return df


def unique_pk(df, table_name, primary_key):
    """
    Data Quality Check -- unique primary key in input
    
    Parameters:
        df: DataFrame
        table_name (string): Table Name to be printed to console
        primary_key (string): primary key column name as string
    
    Returns:
        True if PK is unique, False otherwise
    
    """
    # Check if number of primary keys equals number of rows
    if df[primary_key].nunique() == len(df):
        print('\n' + table_name + ' TABLE Primary Key is unique.')
        print('LOADING ' + table_name + ' TABLE...\n')
        return True
    else: 
        print('\n' + table_name + ' TABLE Primary Key is not unique.\n')
        print('0 records inserted.\n')
        return False
   

def df_to_postgres(cur, conn, df, insert_string):
    """
    Insert DataFrame to Postgres Table
    
    Parameters:
        cur: psycopg2 cursor object
        conn: psycopg2 connection
        df: Input DataFrame
        insert_string: SQL insert string
    
    Returns: None
    """
    # Convert df values to list
    data_list = df.values.tolist()
    
    # Insert by iterating through list items
    for i, item in enumerate(data_list, 1):
        cur.execute(insert_string, item)
        conn.commit()
        
        # Print to console confirmation of every 1000 inserts and completion
        if i % 1000 == 0 or i == len(data_list):
            print('{}/{} records inserted.'.format(i, len(data_list)))
    
    print('\n=====================================================')


def process_song_file(cur, conn, filepath):
    """
    Processes files in song folder and insert into Postgres
    
    Parameters:
        cur: psycopg2 cursor object
        conn: psycopg2 connection
        filepath (string): parent folder path
    
    Returns: None
    """
    # Open song file
    song_files = get_files(filepath)
    df = json_to_df(song_files)
    
    # Create song DataFrame
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].drop_duplicates()
    
    # Insert song records in primary key is unique
    if unique_pk(song_data, 'SONG', 'song_id'):
        df_to_postgres(cur, conn, song_data, songs_table_insert)
       
    # Create artist DataFrame
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].drop_duplicates()
    artist_data.columns = ['artist_id', 'name', 'location', 'latitude', 'longitude']
  
    # insert artist records
    if unique_pk(artist_data, 'ARTIST', 'artist_id'):
        df_to_postgres(cur, conn, artist_data, artists_table_insert)


def process_log_file(cur, conn, filepath):
    """
    Processes files in log folder and insert into Postgres
    
    Parameters:
        cur: psycopg2 cursor object
        conn: psycopg2 connection
        filepath (string): parent folder path
    
    Returns: None
    """
    # Open log file
    log_files = get_files(filepath)
    df = json_to_df(log_files)

    # Filter by NextSong action
    df = df[df['page'] == 'NextSong']
    
    # Convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit = 'ms')
    
    # Create calendar columns
    df['hour'] = t.dt.hour
    df['day'] = t.dt.day
    df['week'] = t.dt.week
    df['month'] = t.dt.month
    df['year'] = t.dt.year
    df['weekday'] = t.dt.weekday
    df['timestamp'] = t.astype(str)
    
    # Insert time data records
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_data = df[column_labels].drop_duplicates()

    if unique_pk(time_data, 'TIME', 'timestamp'):
        df_to_postgres(cur, conn, time_data, time_table_insert)

    # load user DataFrame
    users_data = df[['userId', 'firstName', 'lastName', 'gender', 'level','timestamp']].drop_duplicates()
    users_data = users_data[users_data['userId'] != '']
    
    # Find latest timestamp for each user
    user_max_timestamp = pd.DataFrame(users_data.groupby(['userId'])['timestamp'].max())
    user_max_timestamp.reset_index(level=0, inplace=True)

    # Filter user data with latest timestamps for each user
    users_data = pd.merge(users_data, user_max_timestamp, how = 'inner', on = ['userId', 'timestamp'])
    users_data = users_data.drop(columns = ['timestamp'])
    
    # Check if user primary key is unique, then insert
    if unique_pk(users_data, 'USER', 'userId'):
        df_to_postgres(cur, conn, users_data, users_table_insert)

    # Insert songplay records
    songplays_df = df[['timestamp', 'userId', 'level', 'song', 'length', 'artist', 'sessionId', 'location', 'userAgent']].drop_duplicates()
    songplays_df = songplays_df.sort_values(by = ['timestamp', 'sessionId', 'song'])
    
    print('\nLoading SONGPLAY TABLE...\n')
    
    i = 0
    for index, row in songplays_df.iterrows():

        # Get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # Insert songplay record
        songplay_data = (row.timestamp, row.userId, row.level, songid, row.song, row.length, artistid, row.artist, row.sessionId, row.location, row.userAgent)
        cur.execute(songplays_table_insert, songplay_data)
        conn.commit()
        
        # Print to console confirmation of every 1000 inserts and completion
        i += 1
        if i % 1000 == 0 or i == len(songplays_df):
            print('{}/{} records inserted.'.format(i, len(songplays_df)))
        
    print('\nSUCCESS!')
    print('\n=====================================================\n')


def main():
    
    # Connect to database    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    print('\n\nSparkifyDB ETL')

    process_song_file(cur, conn, filepath='data/song_data')
    process_log_file(cur, conn, filepath='data/log_data')
    
    conn.close()

    
if __name__ == "__main__":
    main()