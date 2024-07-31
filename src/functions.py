import os
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

def getAllData(tableName, page=1, per_page=20):
    try:
        dataBase.connect()
        offset = (page - 1) * per_page
        records, total_count = dataBase.selectAll(tableName, limit=per_page, offset=offset)
        dataBase.disconnect()
        return records, total_count
    except Exception as e:
        raise e


def insertCsvData(tableName, csvFile):
    try:
        dataBase.connect()
        dataBase.insertCsvFile(tableName, csvFile)
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
