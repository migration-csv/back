import psycopg2
from psycopg2 import Error


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

    def selectAll(self, tableName, limit=None, offset=None):
        try:
            column_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            """
            self.__cursor.execute(column_query, (tableName,))
            columns = [row[0] for row in self.__cursor.fetchall()]
            
            query = f"SELECT * FROM {tableName}"
            if limit is not None and offset is not None:
                query += f" LIMIT %s OFFSET %s"
                self.__cursor.execute(query, (limit, offset))
            else:
                self.__cursor.execute(query)
            rows = self.__cursor.fetchall()
            
            records = [dict(zip(columns, row)) for row in rows]
            
            count_query = f"SELECT COUNT(*) FROM {tableName}"
            self.__cursor.execute(count_query)
            total_count = self.__cursor.fetchone()[0]
            
            return records, total_count
        except (Exception, Error) as error:
            print("Error in selectAll:", error)
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
        
    def searchMovies(self, genres, min_rating, user_id, year, page=1, per_page=20):
        try:
            offset = (page - 1) * per_page
            query = '''
                SELECT mov.movieid, mov.title, mov.genres, rat.rating, rat.userid
                FROM movies AS mov
                INNER JOIN ratings AS rat 
                ON mov.movieId = rat.movieId
                WHERE 1=1
            '''
            params = []
            
            if genres:
                genres_placeholder = ', '.join(['%s'] * len(genres))
                query += f" AND EXISTS (SELECT 1 FROM unnest(string_to_array(mov.genres, '|')) AS genre WHERE genre = ANY(ARRAY[{genres_placeholder}]))"
                params.extend(genres)
            
            if min_rating is not None:
                query += " AND rat.rating >= %s"
                params.append(min_rating)
            
            if user_id is not None:
                query += " AND rat.userid::text LIKE %s"
                params.append(f'%{user_id}%')
            
            if year is not None:
                query += " AND mov.title LIKE %s"
                params.append(f'%{year}%')

            query += " LIMIT %s OFFSET %s"
            params.extend([per_page, offset])
            
            self.__cursor.execute(query, tuple(params))
            results = self.__cursor.fetchall()
            
            count_query = '''
                SELECT COUNT(*)
                FROM movies AS mov
                INNER JOIN ratings AS rat 
                ON mov.movieId = rat.movieId
                WHERE 1=1
            '''
            
            count_params = []
            
            if genres:
                count_query += f" AND EXISTS (SELECT 1 FROM unnest(string_to_array(mov.genres, '|')) AS genre WHERE genre = ANY(ARRAY[{genres_placeholder}]))"
                count_params.extend(genres)
            
            if min_rating is not None:
                count_query += " AND rat.rating >= %s"
                count_params.append(min_rating)
            
            if user_id is not None:
                count_query += " AND rat.userid::text LIKE %s"
                count_params.append(f'%{user_id}%')
            
            if year is not None:
                count_query += " AND mov.title LIKE %s"
                count_params.append(f'%{year}%')
            
            self.__cursor.execute(count_query, tuple(count_params))
            total_count = self.__cursor.fetchone()[0]
            
            return results, total_count
        except (Exception, Error) as error:
            raise error
