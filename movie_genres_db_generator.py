import psycopg2
import json
import pandas as pd
import sys

# Connection parameters - update these with your database credentials
db_name = "simple_movies"
db_user = "postgres"
db_password = "nayeel12"
db_host = "localhost"
db_port = "5432"

csv_file_path = "./tmdb_5000_movies.csv"

# Connect to the default database to create the new database
conn = psycopg2.connect(
    dbname="postgres",  # connect to the default 'postgres' database
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
conn.autocommit = True  # enable autocommit for database creation
cur = conn.cursor()

# Create a new database
try:
    cur.execute(f"CREATE DATABASE {db_name};")
    print(f"Database {db_name} created successfully.")
except psycopg2.errors.DuplicateDatabase:
    print(f"Database {db_name} already exists.")
    sys.exit()
except Exception as e:
    print(f"An error occurred while creating the database: {e}")
    sys.exit()

# Close connection to the default database
cur.close()
conn.close()

# Connect to the newly created database
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
conn.autocommit = True  # enable autocommit for table creation
cur = conn.cursor()

# Create tables
try:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
            id SERIAL PRIMARY KEY,
            budget BIGINT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Genres (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS MovieGenres (
            movie_id INTEGER REFERENCES Movies(id),
            genre_id INTEGER REFERENCES Genres(id),
            PRIMARY KEY (movie_id, genre_id)
        );
    """)
    print("Tables created successfully.")
except Exception as e:
    print(f"An error occurred while creating tables: {e}")

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file_path)

# Insert data into the tables
try:
    for index, row in df.iterrows():
        movie_budget = row['budget']
        genres_json = row['genres']

        # Insert the movie and get its ID
        cur.execute("INSERT INTO Movies (budget) VALUES (%s) RETURNING id;", (movie_budget,))
        movie_id = cur.fetchone()[0]

        # Parse the JSON genres and insert them if they don't exist already
        genres = json.loads(genres_json)
        for genre in genres:
            cur.execute("SELECT id FROM Genres WHERE id = %s;", (genre['id'],))
            if cur.fetchone() is None:
                cur.execute("INSERT INTO Genres (id, name) VALUES (%s, %s);", (genre['id'], genre['name']))
            
            # Insert the relationship into MovieGenres
            cur.execute("INSERT INTO MovieGenres (movie_id, genre_id) VALUES (%s, %s);", (movie_id, genre['id']))

    # Commit the changes to the database
    conn.commit()
except Exception as e:
    print(f"An error occurred while inserting data: {e}")
    conn.rollback()
finally:
    # Close the cursor and connection
    cur.close()
    conn.close()

print("Data inserted successfully.")

