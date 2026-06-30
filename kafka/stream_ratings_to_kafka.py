import os, json
from time import sleep
from random import random
from dotenv import load_dotenv
from datetime import datetime

import shapely
from shapely import Point
from shapely.geometry import shape

import requests
from confluent_kafka import Producer

####################################
# download Polygons (using Nominatim)
##################
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

CITY_SPECS = {
    "Paris":  {"q": "Paris",  "countrycodes": "fr"},
    "Rome":   {"q": "Rome",   "countrycodes": "it"},
    "Berlin": {"q": "Berlin", "countrycodes": "de"},
    "Munich": {"q": "Munich", "countrycodes": "de"},
    "Vienna": {"q": "Vienna", "countrycodes": "at"},
    "Zurich": {"q": "Zurich", "countrycodes": "ch"},
}

def download_city_polygon(city, spec, cache_dir):
    os.makedirs(cache_dir, exist_ok=True)
    out_path = os.path.join(cache_dir, f"{city}.geojson")
    if os.path.exists(out_path):
        return out_path
    
    params = {
        "format": "jsonv2",
        "limit": 1,
        "polygon_geojson": 1,
        "addressdetails": 0,
        "q": spec["q"],
        "countrycodes": spec["countrycodes"],
        "featureType": "city",
        "polygon_threshold": 0.0005,
    }
    
    headers = {
        "User-Agent": "fhwn-student/bigdata_project/1.0 (contact: 120323@fhwn.ac.at)"
    }
    
    r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=(5, 30))
    r.raise_for_status()
    results = r.json()
    if not results:
        raise RuntimeError(f"Nominatim returned no results for {city}")

    geo = results[0].get("geojson")
    if not geo:
        raise RuntimeError(f"Nominatim result for {city} has no geojson polygon (maybe not a polygon result)")

    # Save just the geometry object (Polygon/MultiPolygon)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(geo, f)

    sleep(1.0)

    return out_path
####################################


####################################
# load and interpret Polygons (using shapely)
##################
def load_city_geometries(cache_dir: str) -> dict[str, object]:
    geoms = {}
    for city, spec in CITY_SPECS.items():
        path = download_city_polygon(city, spec, cache_dir)
        with open(path, "r", encoding="utf-8") as f:
            geom_geojson = json.load(f)
        geom = shape(geom_geojson)
        shapely.prepare(geom)
        geoms[city] = geom
    return geoms

def city_from_polygons(lat: float, lon: float, city_geoms: dict[str, object]) -> str | None:
    pt = Point(lon, lat)
    for city, geom in city_geoms.items():
        if shapely.covers(geom, pt):  # includes boundary
            return city
    return None
####################################

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def write_as_json(url, topic):
    kafka_config = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
    }

    producer = Producer(kafka_config)
    session = requests.Session()

    send_delay = 0.1
    while True:
        # Fetch data safely        
        try:
            resp = session.get(url, timeout=(5, 30))  # (connect timeout, read timeout)
            resp.raise_for_status()
        except Exception as e:
            print(e)
            sleep(1)
            continue # skip current iteration
        
        data = resp.json()
        
        # dataformat: 
        # [
        #    {"movieId":"2916","rating":"3.0","datetime":"2025-12-19 10:00:25","longitude":2.186700242470983,"latitude":48.8724647323359},
        #    {"movieId":"68358","rating":"2.0","datetime":"2025-12-19 10:00:25","longitude":12.799948387140978,"latitude":42.03948681063349},
        #    {"movieId":"459","rating":"3.0","datetime":"2025-12-19 10:00:25","longitude":4.257535133079015,"latitude":50.72036079052673},
        #    {"movieId":"5989","rating":"4.0","datetime":"2025-12-19 10:00:25","longitude":4.827802964939792,"latitude":52.21836887411483},
        #    {"movieId":"1183","rating":"4.0","datetime":"2025-12-19 10:00:25","longitude":23.35339516747711,"latitude":42.633331014198276}
        # ]

        for row in data:
            current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            print(f"<{current_time_str}> {row}")
            
            row["city"] = city_from_polygons(row["latitude"], row["longitude"], CITY_GEOMS) or "Unknown"

            payload = json.dumps(row)

            # Produce + poll to serve delivery callbacks and avoid queue buildup
            # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
            try:
                producer.produce(topic, value=payload, callback=delivery_report)
            except BufferError:
                # Local queue full: let producer send queued messages, then retry
                producer.poll(1) # 1 second for sending, etc.
                producer.produce(topic, value=payload, callback=delivery_report)

            producer.poll(0)  # serve callbacks/events
            sleep(send_delay)

        # flush occasionally, not every message
        producer.flush(5)  # waits for delivery up to 5 seconds

        sleep(1 - send_delay*len(data)) # small pause, to pull exactly every second from the API endpoint


if __name__ == "__main__":
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env")) # in our docker compose, we will just point to .env directly
    
    CITY_GEOMS = load_city_geometries(cache_dir=os.environ.get("CITY_POLYGON_DIR", "./city_polygons"))

    # Default endpoint for local development.
    # Replace it with your deployed MovieLens API endpoint if required.
    http_endpoint = "http://localhost:8080"

    topic = os.environ.get("TOPIC_RAW", "raw")
    write_as_json(http_endpoint, topic)