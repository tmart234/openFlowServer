import sqlite3
import json
from bottle import Bottle, request, response
from waitress import serve

app = Bottle()
DB_PATH = '{{ db_path }}'

@app.route('/data')
def get_data():
    start_date = request.query.get('start_date')
    end_date = request.query.get('end_date')
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''SELECT * FROM processed_data 
                      WHERE date BETWEEN ? AND ?''', (start_date, end_date))
    data = cursor.fetchall()
    conn.close()
    
    response.content_type = 'application/json'
    return json.dumps([{'date': row[0], 'location': row[1], 'smap_value': row[2], 'vegdri_value': row[3]} 
                       for row in data])

if __name__ == "__main__":
    serve(app, host='0.0.0.0', port=8080)