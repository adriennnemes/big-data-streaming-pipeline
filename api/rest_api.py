import os
from flask import Flask, jsonify
from google.cloud import bigquery

app = Flask(__name__)

# BigQuery settings
PROJECT_ID = "prj-fhwn-2025-schimandl"
DATASET = "movies"
TABLE = "aggregates"

# Initialize BigQuery client
try:
    client = bigquery.Client(project=PROJECT_ID)
    print("BigQuery client initialized")
except Exception as e:
    print(f"BigQuery error: {e}")
    client = None

@app.route('/')
def home():
    return jsonify({
        "message": "Ratings Per Minute API",
        "status": "running",
        "endpoints": {
            "/health": "Health check",
            "/ratings/<city>": "Get ratings for a city (e.g., /ratings/Vienna)",
            "/ratings/all": "Get all cities"
        }
    })

@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "bigquery_ok": client is not None
    })

@app.route('/ratings/<city>')
def get_city_ratings(city):
    if not client:
        return jsonify({"error": "BigQuery not available"}), 500
    
    query = f"""
        SELECT 
            timestamp,
            JSON_EXTRACT_SCALAR(value, '$.city') as city,
            JSON_EXTRACT_SCALAR(value, '$.window_start') as window_start,
            CAST(JSON_EXTRACT_SCALAR(value, '$.ratings_per_minute') AS INT64) as rpm
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE JSON_EXTRACT_SCALAR(value, '$.city') = @city
        ORDER BY timestamp DESC
        LIMIT 1
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("city", "STRING", city)]
    )
    
    try:
        results = client.query(query, job_config=job_config).result()
        for row in results:
            return jsonify({
                "city": row.city,
                "window_start": row.window_start,
                "ratings_per_minute": row.rpm,
                "timestamp": str(row.timestamp)
            })
        return jsonify({"error": f"No data for {city}"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/ratings/all')
def get_all_ratings():
    if not client:
        return jsonify({"error": "BigQuery not available"}), 500
    
    query = f"""
        WITH ranked AS (
            SELECT 
                timestamp,
                JSON_EXTRACT_SCALAR(value, '$.city') as city,
                CAST(JSON_EXTRACT_SCALAR(value, '$.ratings_per_minute') AS INT64) as rpm,
                ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(value, '$.city') ORDER BY timestamp DESC) as rn
            FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        )
        SELECT city, rpm, timestamp
        FROM ranked
        WHERE rn = 1
        ORDER BY city
    """
    
    try:
        results = client.query(query).result()
        cities = [{"city": r.city, "ratings_per_minute": r.rpm, "timestamp": str(r.timestamp)} for r in results]
        return jsonify({"cities": cities, "count": len(cities)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("Starting API on port 5000...")
    app.run(host='0.0.0.0', port=5000, debug=True)