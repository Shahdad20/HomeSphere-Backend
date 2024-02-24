from flask import Flask, jsonify
from flask_cors import CORS 
import Congestion_Map

app = Flask(__name__)
CORS(app)

@app.route('/api/congestion_map')
def get_congestion_map_data():
    data = Congestion_Map.get_data()
    return jsonify(data.to_json())

if __name__ == '__main__':
    app.run(debug=True)
