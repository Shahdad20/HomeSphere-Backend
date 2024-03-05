from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import Congestion_Map
import property_value__per_community
import Housing_Development_Zone
import vacancy_per_community

app = FastAPI()

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/api/congestion_map')
async def get_congestion_map_data():
    data = Congestion_Map.get_data()
    return data.to_json()

@app.get('/api/house_price_map')
async def get_house_price_map_data():
    data = property_value__per_community.get_data()
    return data.to_json()

@app.get('/api/housing_development_zone')
async def get_housing_development_zone_data():
    data = Housing_Development_Zone.get_data()
    return data.to_json()

@app.get('/api/community_vacancy')
async def get_community_vacancy_data():
    data = vacancy_per_community.get_data()
    return data.to_json()
