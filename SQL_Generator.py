import pandas as pd
from google.colab import files

df = pd.read_csv('dataset.csv')

df.dropna(subset=['track_id', 'track_name', 'album_name'], inplace=True)
df.drop_duplicates(subset=['track_id', 'track_name', 'album_name'], inplace=True)

# Split artists and normalize the data
df['artists'] = df['artists'].str.split(';')
artists_expanded = df[['artists']].explode('artists').drop_duplicates().reset_index(drop=True)
artists_expanded['artist_id'] = ['A' + str(i + 1) for i in range(len(artists_expanded))]
artists_expanded = artists_expanded[['artist_id', 'artists']]
artists_expanded.columns = ['artist_id', 'artist_name']
artists_expanded.to_csv('artists.csv', index=False)

# Create Albums table
albums_df = df[['album_name']].drop_duplicates().reset_index(drop=True)
albums_df['album_id'] = ['AL' + str(i + 1) for i in range(len(albums_df))]
albums_df = albums_df[['album_id', 'album_name']]
albums_df.to_csv('albums.csv', index=False)

# Create Album-Artists Linking Table
album_artists_df = df[['album_name', 'artists']].explode('artists').merge(artists_expanded, left_on='artists', right_on='artist_name')
album_artists_df = album_artists_df.merge(albums_df, on='album_name')
album_artists_df = album_artists_df[['album_id', 'artist_id']].drop_duplicates()
album_artists_df.to_csv('album_artists.csv', index=False)

# Create Tracks table with cleaned data and proper join
tracks_df = df[['track_id', 'track_name', 'album_name', 'popularity', 'duration_ms', 'explicit']]
tracks_df = tracks_df.merge(albums_df, on='album_name', how='inner')  # Use inner join to exclude unmatched rows
tracks_df = tracks_df[['track_id', 'track_name', 'album_id', 'popularity', 'duration_ms', 'explicit']]
tracks_df.dropna(subset=['track_id', 'track_name', 'album_id'], inplace=True)  # Ensure critical columns are not null
tracks_df = tracks_df.drop_duplicates(subset=['track_id', 'track_name'])  # Ensure no duplicate track_ids
tracks_df.to_csv('tracks.csv', index=False)

# Create Track-Artists Linking Table
track_artists_df = df[['track_id', 'artists']].explode('artists').merge(artists_expanded, left_on='artists', right_on='artist_name')
track_artists_df = track_artists_df[['track_id', 'artist_id']].drop_duplicates()
track_artists_df.to_csv('track_artists.csv', index=False)

# Create Audio Features table
# Create Audio Features table
audio_features_df = df[['track_id', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
                        'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature', 'track_genre']]
audio_features_df = audio_features_df.dropna(subset=['track_id'])  # Ensure critical columns are not null
audio_features_df = audio_features_df.drop_duplicates(subset=['track_id'])  # Remove duplicate track_ids
audio_features_df.to_csv('audio_features.csv', index=False)


# Download the CSV files
files.download('artists.csv')
files.download('albums.csv')
files.download('album_artists.csv')
files.download('tracks.csv')
files.download('track_artists.csv')
files.download('audio_features.csv')

# Generate Create and Insert SQL Script
def save_insert_queries(dataframe, table_name, file):
    for _, row in dataframe.iterrows():
        columns = ', '.join(dataframe.columns)
        values_list = []
        for x in row.values:
            if pd.isna(x):
                values_list.append("NULL")
            elif isinstance(x, str):
                escaped_x = x.replace("'", "''")
                values_list.append(f"'{escaped_x}'")
            else:
                values_list.append(str(x))
        values = ', '.join(values_list)
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING;\n"
        file.write(insert_query)

# Save the insert queries for each table to the same SQL file
with open('create_and_insert_tables.sql', 'w') as f:
    # Write Create Table Scripts
    f.write("-- Create Artists Table\n")
    f.write("""
    CREATE TABLE artists (
        artist_id VARCHAR PRIMARY KEY,
        artist_name VARCHAR NOT NULL
    );
    """
    )
    f.write("\n\n-- Create Albums Table\n")
    f.write("""
    CREATE TABLE albums (
        album_id VARCHAR PRIMARY KEY,
        album_name VARCHAR NOT NULL
    );
    """)
    f.write("\n\n-- Create Album-Artists Table\n")
    f.write("""
    CREATE TABLE album_artists (
        album_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        FOREIGN KEY (album_id) REFERENCES albums(album_id),
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
        PRIMARY KEY (album_id, artist_id)
    );
    """)
    f.write("\n\n-- Create Tracks Table\n")
    f.write("""
    CREATE TABLE tracks (
        track_id VARCHAR PRIMARY KEY,
        track_name VARCHAR NOT NULL,
        album_id VARCHAR NOT NULL,
        popularity INTEGER,
        duration_ms INTEGER,
        explicit BOOLEAN,
        FOREIGN KEY (album_id) REFERENCES albums(album_id)
    );
    """)
    f.write("\n\n-- Create Track-Artists Table\n")
    f.write("""
    CREATE TABLE track_artists (
        track_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        FOREIGN KEY (track_id) REFERENCES tracks(track_id),
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
        PRIMARY KEY (track_id, artist_id)
    );
    """)
    f.write("\n\n-- Create Audio Features Table\n")
    f.write("""
    CREATE TABLE audio_features (
        track_id VARCHAR PRIMARY KEY,
        danceability FLOAT,
        energy FLOAT,
        key INTEGER,
        loudness FLOAT,
        mode INTEGER,
        speechiness FLOAT,
        acousticness FLOAT,
        instrumentalness FLOAT,
        liveness FLOAT,
        valence FLOAT,
        tempo FLOAT,
        time_signature INTEGER,
        track_genre VARCHAR,
        FOREIGN KEY (track_id) REFERENCES tracks(track_id)
    );
    """)

    # Write Insert Data Statements
    f.write("\n\n-- Insert Data into Artists Table\n")
    save_insert_queries(artists_expanded, 'artists', f)

    f.write("\n\n-- Insert Data into Albums Table\n")
    save_insert_queries(albums_df, 'albums', f)

    f.write("\n\n-- Insert Data into Album-Artists Table\n")
    save_insert_queries(album_artists_df, 'album_artists', f)

    f.write("\n\n-- Insert Data into Tracks Table\n")
    save_insert_queries(tracks_df, 'tracks', f)

    f.write("\n\n-- Insert Data into Track-Artists Table\n")
    save_insert_queries(track_artists_df, 'track_artists', f)

    f.write("\n\n-- Insert Data into Audio Features Table\n")
    save_insert_queries(audio_features_df, 'audio_features', f)

# Download SQL file
files.download('create_and_insert_tables.sql')
