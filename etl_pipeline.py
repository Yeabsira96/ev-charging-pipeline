# ===========================================
# ETLPipeline.py
# This file downloads EV charging station data
# ===========================================

# IMPORTS - These are like loading tools into your workshop
import requests  # Tool to download data from the internet
import pandas as pd  # Tool to work with tables of data (like Excel)
import time  # Tool to add delays (so we don't spam servers)
from database import init_db, load_to_db
import json
import os

# ===========================================
# FUNCTION 1: EXTRACT - Download the data with pagination
# ===========================================
def extract_charging_stations(max_pages=None):
    """
    This function downloads charging station data from Open Charge Map API.
    It automatically handles pagination to get ALL stations in China.
    """
    
    # The web address where the data lives
    url = "https://api.openchargemap.io/v3/poi/"
    
    # YOUR API KEY - Get one free at: https://openchargemap.org/site/develop/api
    api_key = "5a8ffe68-b7e8-424c-9ed8-96642e256d54"  # Replace this with your actual API key
    
    # Add headers to make the request appear legitimate
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    
    # Create an empty list to store ALL stations across all pages
    all_stations = []
    skip = 0  # Start from the first station
    page = 0
    
    while True:
        page += 1
        print(f"📥 Fetching page {page}... (offset: {skip})")
        
        # What we're asking for:
        params = {
            "output": "json",           # Give me data in JSON format (structured text)
            "countrycode": "CN",        # Only stations in China
            "maxresults": 500,          # Get 500 per request (API max)
            "compact": "true",          # Keep the response small
            "verbose": "false",         # Don't give extra details
            "skip": skip,               # Skip previous results (pagination)
            "key": api_key              # Include the API key
        }
        
        # Send the request and get the response
        response = requests.get(url, params=params, headers=headers, timeout=30)
        
        # Check if the request was successful
        if response.status_code != 200:
            print(f"❌ API Error: Status code {response.status_code}")
            print(f"Response: {response.text}")
            raise Exception(f"API request failed with status code {response.status_code}")
        
        # Check if response has content
        if not response.text:
            print("❌ API returned empty response")
            break
        
        # Convert the response to Python-readable format
        data = response.json()
        
        # If no data returned, we've reached the end
        if len(data) == 0:
            print(f"✅ Reached the end of data at page {page}")
            break
        
        print(f"   ✅ Got {len(data)} stations from this page")
        
        # Loop through each station in the data
        for station in data:
            # Extract the information we care about
            station_info = {
                "id": station["ID"],  # Unique ID for each station
                "name": station["AddressInfo"]["Title"],  # Name of the station
                "latitude": station["AddressInfo"]["Latitude"],  # GPS coordinate
                "longitude": station["AddressInfo"]["Longitude"],  # GPS coordinate
                "address": station["AddressInfo"]["AddressLine1"],  # Street address
                "operator": station.get("OperatorInfo", {}).get("Title", "Unknown"),
                # Who runs this station (Tesla, government, etc.)
                "last_updated": station.get("DateLastConfirmed") or station.get("DateLastStatusUpdate") or "Unknown"
                # When was this info last checked?
            }
            
            # Add this station to our list
            all_stations.append(station_info)
        
        # Move to next page
        skip += 500
        
        # Optional: limit pages for testing (remove or set to None for all data)
        if max_pages and page >= max_pages:
            print(f"⚠️  Stopped at page {page} (max_pages limit reached)")
            break
        
        # Be nice to the API - wait 1 second between requests
        time.sleep(1)
    
    # Convert the list of stations into a pandas DataFrame (a table)
    df = pd.DataFrame(all_stations)
    
    return df


# ===========================================
# FUNCTION 2: TRANSFORM - Clean and organize the data
# ===========================================
def transform_stations(df):
    """
    This function cleans up the raw data:
    - Removes duplicates
    - Standardizes operator names (Tesla vs "Tesla Motors Inc.")
    - Flags offline stations
    - Caches geocoding results to avoid slow API calls
    """
    
    print("\n🔧 Starting data transformation...")
    
    # STEP 1: Remove duplicate stations (same ID appearing twice)
    print(f"   Before removing duplicates: {len(df)} stations")
    df = df.drop_duplicates(subset=["id"])
    print(f"   After removing duplicates: {len(df)} stations")
    
    # STEP 2: Clean up operator names
    print("   Standardizing operator names...")
    df["operator_clean"] = df["operator"].str.lower()  # Convert to lowercase
    
    # If operator contains "tesla", change it to just "Tesla"
    df.loc[df["operator_clean"].str.contains("tesla", na=False), "operator_clean"] = "Tesla"
    
    # If operator contains "bp", change to "BP"
    df.loc[df["operator_clean"].str.contains("bp", na=False), "operator_clean"] = "BP"
    
    # If operator contains "shell", change to "Shell"
    df.loc[df["operator_clean"].str.contains("shell", na=False), "operator_clean"] = "Shell"
    
    # Everything else becomes "Other"
    df.loc[~df["operator_clean"].isin(["tesla", "bp", "shell"]), "operator_clean"] = "Other"
    
    # STEP 3: Use reverse geocoding with caching (to avoid slow API calls for large datasets)
    print("   Converting GPS coordinates to city names (cached)...")
    
    # Load or create geocoding cache
    cache_file = "geocoding_cache.json"
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            geocode_cache = json.load(f)
        print(f"   📦 Loaded geocoding cache with {len(geocode_cache)} entries")
    else:
        geocode_cache = {}
    
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="ev_pipeline_yeabsira")
    
    # Function to get city from latitude and longitude (with caching)
    def get_city_cached(lat, lon):
        # Create a cache key from the coordinates
        key = f"{lat:.3f},{lon:.3f}"  # Round to 3 decimals (about 100m accuracy)
        
        # Check if we already have this cached
        if key in geocode_cache:
            return geocode_cache[key]
        
        try:
            # Ask the geocoder: "What's at these coordinates?"
            location = geolocator.reverse(f"{lat}, {lon}", language="en", timeout=10)
            
            # Get the raw address data
            address = location.raw["address"]
            
            # Try to get city, if not available try town, if not available return "Unknown"
            city = address.get("city", address.get("town", "Unknown"))
            
            # Cache the result
            geocode_cache[key] = city
            
            # Save cache every 50 lookups to avoid losing progress
            if len(geocode_cache) % 50 == 0:
                with open(cache_file, 'w') as f:
                    json.dump(geocode_cache, f)
                print(f"   💾 Cached {len(geocode_cache)} locations")
            
            return city
        except Exception as e:
            # If something goes wrong, return "Unknown" and cache it
            geocode_cache[key] = "Unknown"
            return "Unknown"
    
    # Apply the get_city function to each row
    # Process in batches to avoid overwhelming the server
    print(f"   🔄 Geocoding {len(df)} stations (this may take a while)...")
    df["city"] = df.apply(
        lambda row: get_city_cached(row["latitude"], row["longitude"]), 
        axis=1
    )
    
    # Save the final cache
    with open(cache_file, 'w') as f:
        json.dump(geocode_cache, f)
    print(f"   ✅ Geocoding complete! Total cached: {len(geocode_cache)}")
    
    # STEP 4: Flag offline stations
    print("   Flagging offline stations...")
    
    # Convert last_updated column to datetime format
    df["last_updated"] = pd.to_datetime(df["last_updated"], errors='coerce')
    
    # Get today's date
    today = pd.Timestamp.now()
    
    # Calculate how many days since last update
    df["days_since_update"] = (today - df["last_updated"]).dt.days
    
    # Flag stations that haven't been updated in 90+ days
    df["is_offline"] = df["days_since_update"] > 90
    
    offline_count = df['is_offline'].sum()
    print(f"   ⚠️  Found {offline_count} offline stations out of {len(df)}")
    
    return df


# ===========================================
# RUN THE FULL ETL PIPELINE
# ===========================================
if __name__ == "__main__":
    # Step 1: Extract
    print("📦 STEP 1: EXTRACTING DATA FROM API")
    print("=" * 50)
    df = extract_charging_stations()  # Remove max_pages limit to get ALL stations
    print(f"\n✅ Extracted {len(df)} charging stations!\n")
    
    # Step 2: Transform
    print("🔧 STEP 2: TRANSFORMING DATA")
    print("=" * 50)
    df_clean = transform_stations(df)
    print(f"✅ Transformed {len(df_clean)} stations!\n")
    
    # Step 3: Load
    print("💾 STEP 3: LOADING TO DATABASE")
    print("=" * 50)
    conn = init_db()
    load_to_db(df_clean, conn)
    conn.close()
    print("✅ ETL pipeline complete!\n")
    
    # Show summary
    print("📊 SUMMARY:")
    print("=" * 50)
    print(f"   Total stations: {len(df_clean)}")
    print(f"   Offline stations: {df_clean['is_offline'].sum()}")
    print(f"   Online stations: {(~df_clean['is_offline']).sum()}")
    print(f"   Tesla stations: {(df_clean['operator_clean'] == 'Tesla').sum()}")
    print(f"   Data saved to: charging_stations.db")
    print(f"   Geocoding cache: geocoding_cache.json")
    print("\n🎯 Show preview of data:")
    print("-" * 50)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 50)
    print(df_clean.head(10))