import os
import time
from dotenv import load_dotenv
from src.connectionDB import DatabaseHandler

load_dotenv(override=True)

BD_USER = os.getenv("BD_USER")
BD_PASSWORD = os.getenv("BD_PASSWORD")
BD_HOST = os.getenv("BD_HOST")
BD_PORT = os.getenv("BD_PORT")
BD_NAME = os.getenv("BD_NAME")

dataBase = DatabaseHandler(
    user=BD_USER,
    password=BD_PASSWORD,
    host=BD_HOST,
    port=BD_PORT,
    database=BD_NAME
)

def connect_db():
    try:
        dataBase.connect()
    except Exception as e:
        raise e

def getAllData(tableName, page=1, per_page=30):
    try:
        dataBase.connect()
        offset = (page - 1) * per_page
        columns, records = dataBase.selectAll(tableName, limit=per_page, offset=offset)
        dataBase.disconnect()
        return columns, records
    except Exception as e:
        raise e

def getFiles():
    try:
        dataBase.connect()
        records = dataBase.selectFiles()
        dataBase.disconnect()
        return records
    except Exception as e:
        raise e

def getTmdbId(movieId):
    try:
        dataBase.connect()
        result = dataBase.getTmdbId(movieId)
        dataBase.disconnect()
        return result
    except Exception as e:
        raise e 

def getColumnsTable(tableName):
    try:
        dataBase.connect()
        columns = dataBase.getColumnsTable(tableName)
        dataBase.disconnect()
        return columns
    except Exception as e:  
        raise e

def searchMovies(genres, min_rating, year,total_ratings, page=1, per_page=30):
    try:
        dataBase.connect()
        dataBase.createIndexSearchMovies()
        records = dataBase.searchMovies(genres, min_rating, year, total_ratings, page, per_page)
        dataBase.disconnect()
        return records
    except Exception as e:
        raise e

def insertCsvData(tableName, csvFile, file_name):
    try:
        dataBase.connect()
        start_time = time.time()
        dataBase.insertCsvFile(tableName, csvFile)
        end_time = time.time()
        execution_time = end_time - start_time
        dataBase.updateExecutionTime(file_name, execution_time)
        dataBase.disconnect()    
    except Exception as e:
        raise e

def insertData(tableName, data):
    try:
        dataBase.connect()
        dataBase.insertRow(tableName, data)
        dataBase.disconnect()
    except Exception as e:
        raise e
    
def deleteData(tableName, file_name):
    try:
        dataBase.connect()
        dataBase.deleteRow(tableName, file_name)
        dataBase.disconnect() 
    except Exception as e:
        raise e
