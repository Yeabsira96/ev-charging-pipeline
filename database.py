# ===========================================
# database.py
# This file handles saving/loading data to SQLite database
# ===========================================

import sqlite3  # Tool to work with SQLite databases
import pandas as pd  # Still need pandas to work with tables
import os  # Tool to check if files exist


# ===========================================
# FUNCTION 1: Initialize the database
# ===========================================
def init_db():
    """
    Creates the database file and the stations table if they don't exist.
    A "table" is like an Excel sheet inside the database.
    Also creates indexes for fast queries on large datasets.
    """
    
    print("\n💾 Initializing database...")
    
    # Connect to the database file (creates it if it doesn't exist)
    conn = sqlite3.connect("charging_stations.db")
    
    # Create a cursor to execute SQL commands
    cursor = conn.cursor()
    
    # SQL command to create the stations table
    # Think of this like defining the columns of an Excel sheet
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stations (
            id INTEGER PRIMARY KEY,        -- Unique ID (like a row number)
            name TEXT,                     -- Station name
            latitude REAL,                 -- GPS coordinate (decimal number)
            longitude REAL,                -- GPS coordinate (decimal number)
            address TEXT,                  -- Street address
            operator_clean TEXT,           -- Cleaned operator name
            city TEXT,                     -- City name
            is_offline BOOLEAN,            -- True/False: is it offline?
            days_since_update INTEGER,     -- How many days since last update
            last_updated TEXT              -- Date of last update
        )
    ''')
    
    # Create indexes for faster queries on large datasets
    # Indexes are like a book's index - they help find data quickly
    print("   🔍 Creating database indexes...")
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_city ON stations(city)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_operator ON stations(operator_clean)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_offline ON stations(is_offline)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_lat_lon ON stations(latitude, longitude)')
    
    # Save the changes
    conn.commit()
    
    print("   ✅ Database initialized with indexes!")
    
    return conn


# ===========================================
# FUNCTION 2: Load data into the database
# ===========================================
def load_to_db(df, conn):
    """
    Takes our cleaned DataFrame and saves it to the database.
    This is the "L" in ETL (Extract, Transform, Load).
    Optimized for large datasets.
    """
    
    print("\n📥 Loading data into database...")
    
    # Get the list of station IDs we're about to insert
    ids = df["id"].tolist()
    
    # Delete any existing records with these IDs (this is called "upsert" - update or insert)
    # We do this so we don't get duplicate entries when we run the pipeline again
    placeholders = ','.join('?' * len(ids))  # Creates "?, ?, ?, ..." for SQL
    conn.execute(f"DELETE FROM stations WHERE id IN ({placeholders})", ids)
    
    # Now insert the new data
    # to_sql() is a pandas function that writes a DataFrame to SQL
    df.to_sql("stations", conn, if_exists="append", index=False)
    
    # Save the changes
    conn.commit()
    
    # Print database stats
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stations")
    total_records = cursor.fetchone()[0]
    
    print(f"   ✅ Loaded {len(df)} records to database!")
    print(f"   📊 Total records in database: {total_records}")


# ===========================================
# FUNCTION 3: Read data from the database
# ===========================================
def read_from_db(conn):
    """
    Reads all stations from the database and returns them as a DataFrame.
    Useful for the dashboard later.
    """
    
    # SQL query to select all rows from the stations table
    query = "SELECT * FROM stations"
    
    # Use pandas to run the query and return a DataFrame
    df = pd.read_sql_query(query, conn)
    
    return df


# ===========================================
# FUNCTION 4: Get statistics for dashboard
# ===========================================
def get_stats(conn):
    """
    Get quick statistics from the database for the dashboard.
    Uses indexes for fast queries.
    """
    cursor = conn.cursor()
    
    stats = {}
    
    # Total stations
    cursor.execute("SELECT COUNT(*) FROM stations")
    stats['total'] = cursor.fetchone()[0]
    
    # Offline stations
    cursor.execute("SELECT COUNT(*) FROM stations WHERE is_offline = 1")
    stats['offline'] = cursor.fetchone()[0]
    
    # Online stations
    cursor.execute("SELECT COUNT(*) FROM stations WHERE is_offline = 0")
    stats['online'] = cursor.fetchone()[0]
    
    # Stations by operator
    cursor.execute("SELECT operator_clean, COUNT(*) as count FROM stations GROUP BY operator_clean")
    stats['by_operator'] = dict(cursor.fetchall())
    
    # Stations by city (top 10)
    cursor.execute("SELECT city, COUNT(*) as count FROM stations WHERE city != 'Unknown' GROUP BY city ORDER BY count DESC LIMIT 10")
    stats['by_city'] = dict(cursor.fetchall())
    
    return stats


# ===========================================
# TEST THE DATABASE FUNCTIONS
# ===========================================
if __name__ == "__main__":
    # Initialize database
    conn = init_db()
    
    # Create a simple test DataFrame
    test_data = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Test Station 1", "Test Station 2", "Test Station 3"],
        "latitude": [31.2, 39.9, 22.3],
        "longitude": [121.4, 116.4, 114.1],
        "address": ["Address 1", "Address 2", "Address 3"],
        "operator_clean": ["Tesla", "Other", "BP"],
        "city": ["Shanghai", "Beijing", "Hong Kong"],
        "is_offline": [False, True, False],
        "days_since_update": [5, 120, 30],
        "last_updated": ["2026-01-01", "2025-09-01", "2025-12-01"]
    })
    
    # Load test data
    load_to_db(test_data, conn)
    
    # Read it back
    result = read_from_db(conn)
    print("\n✅ Database test successful! Retrieved data:")
    print(result)
    
    # Get statistics
    stats = get_stats(conn)
    print("\n📊 Database statistics:")
    print(stats)
    
    # Close the connection
    conn.close()