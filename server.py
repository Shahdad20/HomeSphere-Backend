from flask import Flask, jsonify
from flask_cors import CORS 
import Congestion_Map
import property_value__per_community
import Housing_Development_Zone
import vacancy_per_community

app = Flask(__name__)
CORS(app,origins="*")

@app.route('/api/congestion_map')
def get_congestion_map_data():
    data = Congestion_Map.get_data()
    return jsonify(data.to_json())

@app.route('/api/house_price_map')
def get_house_price_map_data():
    data = property_value__per_community.get_data()
    return jsonify(data.to_json())

@app.route('/api/housing_development_zone')
def get_housing_development_zone_data():
    data = Housing_Development_Zone.get_data()
    return jsonify(data.to_json())

@app.route('/api/community_vacancy')
def get_community_vacancy_data():
    data = vacancy_per_community.get_data()
    return jsonify(data.to_json())

if __name__ == '__main__':
    app.run(debug=True)
