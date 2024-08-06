import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def create_tables():
    db_user = os.getenv("BD_USER")
    db_password = os.getenv("BD_PASSWORD")
    db_host = os.getenv("BD_HOST")
    db_port = os.getenv("BD_PORT")
    db_name = os.getenv("BD_NAME")

    conn_str = f"dbname={db_name} user={db_user} password={db_password} host={db_host} port={db_port}"
    
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id serial primary key,
            file_name varchar(100),
	        update_at TIMESTAMP DEFAULT NOW()
        );
    ''')

    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS ratings (
            userId INTEGER,
            movieId INTEGER,
            rating FLOAT,
            timestamp INTEGER,
            PRIMARY KEY (userId, movieId)
        );
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tags (
            userId INTEGER,
            movieId INTEGER,
            tag TEXT,
            timestamp INTEGER
        );
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS movies (
            movieId INTEGER PRIMARY KEY,
            title TEXT,
            genres TEXT
        );
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS links (
            movieId INTEGER PRIMARY KEY,
            imdbId INTEGER,
            tmdbId INTEGER
        );
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS genome_scores (
            movieId INTEGER,
            tagId INTEGER,
            relevance FLOAT,
            PRIMARY KEY (movieId, tagId)
        );
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS genome_tags (
            tagId INTEGER PRIMARY KEY,
            tag TEXT
        );
    ''')
    cur.execute('''
        CREATE MATERIALIZED VIEW movie_ratings AS
        SELECT 
            mov.movieid, 
            mov.title, 
            mov.genres, 
            AVG(rat.rating) AS avg_rating, 
            COUNT(*) AS rating_count
        FROM movies AS mov
        INNER JOIN ratings AS rat 
        ON mov.movieid = rat.movieid
        GROUP BY mov.movieid, mov.title, mov.genres;
    ''')
    
    conn.commit()
    cur.close()
    conn.close()


