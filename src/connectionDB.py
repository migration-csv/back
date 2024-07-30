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

    def selectAll(self, tableName):
        try:
            self.__cursor.execute(f"SELECT * FROM {tableName}")
            records = self.__cursor.fetchall()
            return records
        except (Exception, Error) as error:
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