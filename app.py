from flask import Flask, request, jsonify
from flask_cors import CORS
from src.functions import *

app = Flask(__name__)
CORS(app)

@app.route('/tables/<table_name>', methods=['GET'])
def get_data(table_name):
    records = getAllData(table_name)
    return jsonify(records), 200 if records else 404

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
        return jsonify({"error": str(e)}), 500

@app.route('/files/<table_name>', methods=['POST'])
def upload_csv(table_name):
    try:
        if 'file' not in request.files:
            return "No file part", 400
        file = request.files['file']
        file_name = file.filename
        if file_name == '':
            return "No selected file", 400
        
        uploads_dir = './uploads'
        if not os.path.exists(uploads_dir):
            os.makedirs(uploads_dir)
        file_path = os.path.join(uploads_dir, file_name)
        file.save(file_path)
        insertCsvData(table_name, file_path)
        insertData("files", file_name)
        
        return "File uploaded and data inserted", 201
    except Exception as e:
        app.logger.error(f"Error processing file: {e}")
        return str(e), 500

@app.route('/files/<table_name>/<id>/', methods=['DELETE'])
def delete_data(table_name, id):
    deleteData(table_name, id)
    return "Row deleted", 200


if __name__ == '__main__':
    app.run(debug=True)
