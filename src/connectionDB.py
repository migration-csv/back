import psycopg2
from psycopg2 import Error

movie_ratings_tables = [
    "movies",
    "ratings"
]

class DatabaseHandler:
    def __init__(self, user, password, host, port, database):
        self.__user = user
        self.__password = password
        self.__host = host
        self.__port = port
        self.__database = database
        self.__connection = None
        self.__cursor = None

    def connect(self):
        try:
            self.__connection = psycopg2.connect(
                user=self.__user,
                password=self.__password,
                host=self.__host,
                port=self.__port,
                database=self.__database
            )
            self.__cursor = self.__connection.cursor()
        except (Exception, Error) as error:
            raise error

    def disconnect(self):
        if self.__connection:
            self.__cursor.close()
            self.__connection.close()

    def selectFiles(self):
        try:
            self.__cursor.execute("SELECT * FROM files")
            records = self.__cursor.fetchall()
            return records
        except (Exception, Error) as error:
            raise error
        
    def createViewMaterialized(self, tableName):
        try:
            view_name = f"{tableName}_view"
            query = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS SELECT * FROM {tableName}"
            self.__cursor.execute(query)
            self.__connection.commit()
            return view_name
        except (Exception, Error) as error:
            self.__connection.rollback()
            raise error

    def selectAll(self, tableName, limit=30, offset=0):
        try:
            view_name = self.createViewMaterialized(tableName)
            query = f"""
                WITH filtered_view AS (
                    SELECT * FROM {view_name}
                )
                SELECT *,
                       (SELECT COUNT(*) FROM filtered_view) AS total_count
                FROM filtered_view
                LIMIT %s OFFSET %s;
            """
            self.__cursor.execute(query, (limit, offset))
            records = self.__cursor.fetchall()
            columns = [desc[0] for desc in self.__cursor.description]
            return columns, records
        except (Exception, Error) as error:
            self.__connection.rollback()
            raise error
    
    def deleteRow(self, tableName, file_name):
        try:
            self.__cursor.execute(f"DELETE FROM {tableName} WHERE file_name = %s", (file_name,))
            self.__connection.commit()

        except (Exception, Error) as error:
            raise error

    
    def insertCsvFile(self, tableName, file):
        try:
            with open(file, 'r') as file:
                self.__cursor.copy_expert(f"COPY {tableName} FROM STDIN WITH CSV HEADER", file)
                self.__connection.commit()
                if tableName in movie_ratings_tables:
                    self.refreshMaterializedView("movie_ratings")
            
        except (Exception, Error) as error:
            raise error
    def insertRow(self, tableName, value):
        try:
            query = f"INSERT INTO {tableName} (file_name) VALUES (%s) RETURNING id"
            self.__cursor.execute(query, (value,))
            self.__connection.commit()
        except (Exception, Error) as error:
            self.__connection.rollback()
            raise error

    def createIndexSearchMovies(self):
        try:
            self.__cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_movies_ratings
                ON movies USING btree (movieId, title);
            """)
            self.__connection.commit()
        except (Exception, Error) as error:
            self.__connection.rollback()
            raise error
    

    def getColumnsTable(self, tableName):
        try:
            self.__cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tableName}'")
            columns = self.__cursor.fetchall()
            return columns
        except (Exception, Error) as error:
            raise error
        
    def getTmdbId(self, movieId):
        self.__cursor.execute(f"select tmdbid from links where movieid = {movieId}")
        result = self.__cursor.fetchone()
        return result

    def refreshMaterializedView(self, view_name):
            try:
                self.__cursor.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
                self.__connection.commit()
            except (Exception, Error) as error:
                self.__connection.rollback()
                raise error
    
    def searchMovies(self, genres, min_rating, year, total_ratings, page=1, per_page=30):
        try:
            offset = (page - 1) * per_page
            
            # Base query with CTE
            query_base = """
                WITH filtered_ratings AS (
                    SELECT movieid, title, genres, avg_rating, rating_count
                    FROM movie_ratings
                    %s
                )
                SELECT 
                    movieid, 
                    title, 
                    genres, 
                    avg_rating, 
                    rating_count, 
                    (SELECT COUNT(*) FROM filtered_ratings) AS total_count
                FROM filtered_ratings
                LIMIT %s OFFSET %s;
            """
            conditions = []
            params = []
            
            if min_rating is not None:
                conditions.append(" WHERE avg_rating >= %s")
                params.append(min_rating)
            
            if genres:
                genre_filters = " AND ".join([f"genres LIKE %s" for _ in genres])
                conditions.append(f"({genre_filters})")
                params.extend([f'%{genre}%' for genre in genres])

            if total_ratings is not None:
                conditions.append("rating_count >= %s")
                params.append(total_ratings)
            
            if year is not None:
                conditions.append("title LIKE %s")
                params.append(f'%({year})%')
            conditions_str = " AND ".join(conditions)
            query = query_base % (conditions_str, per_page, offset) 
            self.__cursor.execute(query, params)
            results = self.__cursor.fetchall()
            return results
        
        except (Exception, Error) as error:
            raise error
