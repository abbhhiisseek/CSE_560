import pandas as pd
from google.colab import files

# Load the dataset
df = pd.read_csv('dataset.csv')

# Clean the data
df.dropna(subset=['track_id', 'track_name', 'album_name'], inplace=True)
df.drop_duplicates(subset=['track_id', 'track_name', 'album_name'], inplace=True)

# Split artists and normalize the data
df['artists'] = df['artists'].str.split(';')
artists_expanded = df[['artists']].explode('artists').drop_duplicates().reset_index(drop=True)
artists_expanded['artist_id'] = ['ART' + str(i + 1).zfill(6) for i in range(len(artists_expanded))]
artists_expanded = artists_expanded[['artist_id', 'artists']]
artists_expanded.columns = ['artist_id', 'artist_name']
artists_expanded.to_csv('artists.csv', index=False)

# Create Albums table
albums_df = df[['album_name']].drop_duplicates().reset_index(drop=True)
albums_df['album_id'] = ['ALB' + str(i + 1).zfill(6) for i in range(len(albums_df))]
albums_df = albums_df[['album_id', 'album_name']]
albums_df.to_csv('albums.csv', index=False)

# Create Album-Artists Linking Table
album_artists_df = df[['album_name', 'artists']].explode('artists').merge(artists_expanded, left_on='artists', right_on='artist_name')
album_artists_df = album_artists_df.merge(albums_df, on='album_name')
album_artists_df = album_artists_df[['album_id', 'artist_id']].drop_duplicates()
album_artists_df.to_csv('album_artists.csv', index=False)

# Create Tracks table
tracks_df = df[['track_id', 'track_name', 'album_name', 'popularity', 'duration_ms', 'explicit']]
tracks_df = tracks_df.merge(albums_df, on='album_name', how='inner')  # Inner join to exclude unmatched rows
tracks_df = tracks_df[['track_id', 'track_name', 'album_id', 'popularity', 'duration_ms', 'explicit']]
tracks_df.dropna(subset=['track_id', 'track_name', 'album_id'], inplace=True)
tracks_df = tracks_df.drop_duplicates(subset=['track_id', 'track_name'])
tracks_df.to_csv('tracks.csv', index=False)

# Create Track-Artists Linking Table
track_artists_df = df[['track_id', 'artists']].explode('artists').merge(artists_expanded, left_on='artists', right_on='artist_name')
track_artists_df = track_artists_df[['track_id', 'artist_id']].drop_duplicates()
track_artists_df.to_csv('track_artists.csv', index=False)

# Create Audio Features table
audio_features_df = df[['track_id', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
                        'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature']]
audio_features_df = audio_features_df.dropna(subset=['track_id'])  # Ensure critical columns are not null
audio_features_df = audio_features_df.drop_duplicates(subset=['track_id'])
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
    CREATE TABLE Artists (
        artist_id VARCHAR(10) PRIMARY KEY,
        artist_name VARCHAR(255) UNIQUE NOT NULL
    );
    CREATE INDEX idx_artist_name ON Artists (artist_name);
    """)

    f.write("\n\n-- Create Albums Table\n")
    f.write("""
    CREATE TABLE Albums (
        album_id VARCHAR(10) PRIMARY KEY,
        album_name VARCHAR(255) UNIQUE NOT NULL
    );
    CREATE INDEX idx_album_name ON Albums (album_name);
    """)

    f.write("\n\n-- Create Album-Artists Table\n")
    f.write("""
    CREATE TABLE AlbumArtists (
        album_id VARCHAR(10) NOT NULL,
        artist_id VARCHAR(10) NOT NULL,
        PRIMARY KEY (album_id, artist_id),
        FOREIGN KEY (album_id) REFERENCES Albums(album_id) ON DELETE CASCADE,
        FOREIGN KEY (artist_id) REFERENCES Artists(artist_id) ON DELETE CASCADE
    );
    """)

    f.write("\n\n-- Create Tracks Table\n")
    f.write("""
    CREATE TABLE Tracks (
        track_id VARCHAR(30) PRIMARY KEY,
        track_name VARCHAR(255) NOT NULL,
        album_id VARCHAR(10) NOT NULL,
        popularity INT CHECK (popularity >= 0 AND popularity <= 100),
        duration_ms INT CHECK (duration_ms > 0),
        explicit BOOLEAN NOT NULL,
        FOREIGN KEY (album_id) REFERENCES Albums(album_id) ON DELETE CASCADE
    );
    CREATE INDEX idx_track_name ON Tracks (track_name);
    CREATE INDEX idx_album_id ON Tracks (album_id);
    """)

    f.write("\n\n-- Create Track-Artists Table\n")
    f.write("""
    CREATE TABLE TrackArtists (
        track_id VARCHAR(30) NOT NULL,
        artist_id VARCHAR(10) NOT NULL,
        PRIMARY KEY (track_id, artist_id),
        FOREIGN KEY (track_id) REFERENCES Tracks(track_id) ON DELETE CASCADE,
        FOREIGN KEY (artist_id) REFERENCES Artists(artist_id) ON DELETE CASCADE
    );
    """)

    f.write("\n\n-- Create Audio Features Table\n")
    f.write("""
    CREATE TABLE AudioFeatures (
        track_id VARCHAR(30) PRIMARY KEY,
        danceability FLOAT CHECK (danceability >= 0.0 AND danceability <= 1.0),
        energy FLOAT CHECK (energy >= 0.0 AND energy <= 1.0),
        key INT,
        loudness FLOAT,
        mode INT,
        speechiness FLOAT CHECK (speechiness >= 0.0 AND speechiness <= 1.0),
        acousticness FLOAT CHECK (acousticness >= 0.0 AND acousticness <= 1.0),
        instrumentalness FLOAT CHECK (instrumentalness >= 0.0 AND instrumentalness <= 1.0),
        liveness FLOAT CHECK (liveness >= 0.0 AND liveness <= 1.0),
        valence FLOAT CHECK (valence >= 0.0 AND valence <= 1.0),
        tempo FLOAT,
        time_signature INT
    );
    CREATE INDEX idx_audio_features_danceability ON AudioFeatures (danceability);
    CREATE INDEX idx_audio_features_energy ON AudioFeatures (energy);
    """)  # Retained indexes on `danceability` and `energy` for performance.

    # Write Insert Data Statements
    f.write("\n\n-- Insert Data into Artists Table\n")
    save_insert_queries(artists_expanded, 'Artists', f)

    f.write("\n\n-- Insert Data into Albums Table\n")
    save_insert_queries(albums_df, 'Albums', f)

    f.write("\n\n-- Insert Data into Album-Artists Table\n")
    save_insert_queries(album_artists_df, 'AlbumArtists', f)

    f.write("\n\n-- Insert Data into Tracks Table\n")
    save_insert_queries(tracks_df, 'Tracks', f)

    f.write("\n\n-- Insert Data into Track-Artists Table\n")
    save_insert_queries(track_artists_df, 'TrackArtists', f)

    f.write("\n\n-- Insert Data into Audio Features Table\n")
    save_insert_queries(audio_features_df, 'AudioFeatures', f)

# Download SQL file
files.download('create_and_insert_tables.sql')
