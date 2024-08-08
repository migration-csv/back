import os
import threading
import queue
from flask import Flask, request, jsonify, send_from_directory, abort
from flask_cors import CORS
from src.functions import *
from src.create_tables import create_tables

app = Flask(__name__)
CORS(app)

UPLOADS_DIR = './uploads'
file_queue = queue.Queue()

create_tables()

@app.route('/tables/<table_name>', methods=['GET'])
def get_data(table_name):
    page = request.args.get('page', default=1, type=int)
    per_page = request.args.get('per_page', default=30, type=int)

    if page < 1 or per_page < 1:
        return jsonify({"error": "Invalid page or per_page parameter"}), 400

    try:
        columns, records = getAllData(table_name, page, per_page)
        total_count_index = len(columns) - 1
        total_count = records[0][total_count_index]
        results = []
        for record in records:
            result = {}
            for i, column in enumerate(columns):
                if column == "total_count":
                    continue
                result[column] = record[i]
            results.append(result)
        columns.pop()
        return jsonify({
            "page": page,
            "per_page": per_page,
            "data": results,
            "total_count": total_count,
            "columns": columns
        })
    except Exception as e:
        return jsonify({"error": "server error"}), 500


@app.route('/search', methods=['GET'])
def search_movies():
    genres_str = request.args.get('genres', '')
    min_rating = request.args.get('min_rating', 1)
    year = request.args.get('year', None)   
    page = request.args.get('page', default=1, type=int)
    per_page = request.args.get('per_page', default=30, type=int)
    total_ratings = request.args.get('total_ratings', None)

    if page < 1 or per_page < 1:
        return jsonify({"error": "Invalid page or per_page parameter"}), 400

    genres = [genre.strip() for genre in genres_str.split(',') if genre.strip()]

    try:
        results = searchMovies(genres, min_rating, year, total_ratings, page, per_page)
        total_count = results[0][5]
        result = [
            {
                "movie_id": record[0],
                "title": record[1],
                "genres": record[2],
                "rating": round(record[3], 2),
                "total_ratings": record[4]
            }
            for record in results
        ]
        
        return jsonify({
            "total_count": total_count,
            "page": page,
            "per_page": per_page,
            "data": result
        })
    except Exception as e:
        app.logger.error(f"Error searching movies: {e}")
        return jsonify({"error": "server error"}), 500


@app.route('/files', methods=['GET'])
def get_files():
    try:
        page = request.args.get('page', default=1, type=int)
        per_page = request.args.get('per_page', default=30, type=int)

        if page < 1 or per_page < 1:
            return jsonify({"error": "Invalid page or per_page parameter"}), 400

        records = getFiles()
        result = [
                {
                    "id": record[0],
                    "file_name": record[1],
                    "update_at": record[2]
                }
                for record in records
            ]
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": "server error"}), 500

@app.route('/files/<table_name>', methods=['POST'])
def upload_csv(table_name):
    if 'file' not in request.files:
        return "No file part", 400

    file = request.files['file']
    if not file.filename:
        return jsonify({"error": "No selected file"}), 400

    try:
        if not os.path.exists(UPLOADS_DIR):
            os.makedirs(UPLOADS_DIR)

        file_path = os.path.join(UPLOADS_DIR, file.filename)
        file.save(file_path)
        file_queue.put((table_name, file_path, file.filename))

        return jsonify({"message": "File uploaded and queued for processing"}), 201
    except Exception as e:
        app.logger.error(f"Error processing file: {e}")
        return jsonify({"error": "server error"}), 500

@app.route('/files/download/<file_name>', methods=['GET'])
def download_file(file_name):
    file_path = os.path.join(UPLOADS_DIR, file_name)
    if os.path.isfile(file_path):
        return send_from_directory(UPLOADS_DIR, file_name, as_attachment=True)
    abort(404)

@app.route('/files/<file_name>/', methods=['DELETE'])
def delete_data(file_name):
    file_path = os.path.join(UPLOADS_DIR, file_name)
    if os.path.exists(file_path):
        os.remove(file_path)
    deleteData("files", file_name)
    return jsonify({"res": "Row deleted"}), 200

@app.route("/movie/get-tmd-id/<movieId>", methods=["GET"])
def get_tmdb_id(movieId):
   tmdbId = getTmdbId(movieId)
   return jsonify({"tmdbId": tmdbId[0]}), 200

@app.route("/tables/columns/<table_name>", methods=["GET"])
def get_columns(table_name):
    try:
        columns = getColumnsTable(table_name)
        array_columns = [ columns for columns, in columns ] 
        return jsonify(array_columns), 200
    except Exception as e:
        print(e)
        return jsonify({"error": "server error"}), 500

def process_files():
    while True:
        table_name, file_path, file_name = file_queue.get()
        try:
            insertData("files", file_name)
            insertCsvData(table_name, file_path)
            app.logger.info(f"File processed: {file_name}")
        except Exception as e:
            app.logger.error(f"Error processing file {file_name}: {e}")
        finally:
            file_queue.task_done()

file_processor_thread = threading.Thread(target=process_files, daemon=True)
file_processor_thread.start()

if __name__ == '__main__':
    app.run(debug=True)
