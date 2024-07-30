import os
import threading
import queue
from flask import Flask, request, jsonify, send_from_directory, abort
from flask_cors import CORS
from src.functions import *

app = Flask(__name__)
CORS(app)

uploads_dir = './uploads'
file_queue = queue.Queue()

@app.route('/tables/<table_name>', methods=['GET'])
def get_data(table_name):
    records = getAllData(table_name)
    if not records:
        return jsonify({"error": "table not found"}), 404
    return jsonify(records)

@app.route('/files', methods=['GET'])
def get_files():
    try:
        records = getAllData("files")
        if records:
            result = [
                {
                    "id": record[0],
                    "file_name": record[1],
                    "update_at": record[2]
                }
                for record in records
            ]
            return jsonify(result), 200
        else:
            return jsonify([]), 404
    except Exception as e:
        return jsonify({"error": "server error"}), 500

@app.route('/files/<table_name>', methods=['POST'])
def upload_csv(table_name):
    try:
        if 'file' not in request.files:
            return "No file part", 400
        file = request.files['file']
        file_name = file.filename
        if file_name == '':
            return jsonify({"res": "No selected file"}), 400
        
        if not os.path.exists(uploads_dir):
            os.makedirs(uploads_dir)
        file_path = os.path.join(uploads_dir, file_name)
        file.save(file_path)
        
        file_queue.put((table_name, file_path, file_name))
        
        return "File uploaded and queued for processing", 201
    except Exception as e:
        app.logger.error(f"Error processing file: {e}")
        return jsonify({"error": "server error"}), 500

@app.route('/files/download/<file_name>', methods=['GET'])
def download_file(file_name):
    try:
        file_path = os.path.join(uploads_dir, file_name)
        if os.path.isfile(file_path):
            return send_from_directory(uploads_dir, file_name, as_attachment=True)
        else:
            abort(404)
    except Exception as e:
        app.logger.error(f"Error downloading file: {e}")
        abort(500)

@app.route('/files/<file_name>/', methods=['DELETE'])
def delete_data(file_name):
    if os.path.exists(os.path.join(uploads_dir, file_name)):
        os.remove(os.path.join(uploads_dir, file_name))
    deleteData("files", file_name)
    return jsonify({"res": "Row deleted"}), 200

def process_files():
    while True:
        table_name, file_path, file_name = file_queue.get()
        try:
            insertCsvData(table_name, file_path)
            insertData("files", file_name)
            app.logger.info(f"File processed: {file_name}")
        except Exception as e:
            app.logger.error(f"Error processing file {file_name}: {e}")
        finally:
            file_queue.task_done()

file_processor_thread = threading.Thread(target=process_files, daemon=True)
file_processor_thread.start()

if __name__ == '__main__':
    app.run(debug=True)
