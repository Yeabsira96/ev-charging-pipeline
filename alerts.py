# ===========================================
# alerts.py
# This file checks for problems and sends alerts
# ===========================================

import sqlite3
import pandas as pd
from database import read_from_db


# ===========================================
# FUNCTION 1: Check offline stations
# ===========================================
def check_offline_stations():
    """
    Checks what percentage of stations are offline.
    If more than 10% are offline, it raises an alert.
    This is like a "check engine" light for the charging network.
    """
    
    print("\n🚨 Checking station health...")
    
    # Connect to database
    conn = sqlite3.connect("charging_stations.db")
    
    # Read all stations
    df = read_from_db(conn)
    
    # Count offline stations
    offline_count = df["is_offline"].sum()
    total_count = len(df)
    
    # Calculate percentage
    offline_pct = (offline_count / total_count) * 100
    
    print(f"   Total stations: {total_count}")
    print(f"   Offline stations: {offline_count}")
    print(f"   Offline percentage: {offline_pct:.1f}%")
    
    # Alert threshold: 10%
    threshold = 10.0
    
    if offline_pct > threshold:
        # ALERT! Too many stations offline
        alert_message = f"""
        ⚠️⚠️⚠️ ALERT ⚠️⚠️⚠️
        {offline_pct:.1f}% of charging stations are offline!
        This is above the {threshold}% threshold.
        Offline count: {offline_count}/{total_count}
        Please investigate immediately.
        """
        print(alert_message)
        
        # In a real system, you'd send an email or Slack message here
        # For now, we just print to console
    else:
        # All good!
        print(f"\n✅ System healthy: {offline_pct:.1f}% offline (below {threshold}% threshold)")
    
    conn.close()
    
    return offline_pct


# ===========================================
# FUNCTION 2: List offline stations
# ===========================================
def list_offline_stations():
    """
    Returns a list of all offline stations with their details.
    Useful for the dashboard and for debugging.
    """
    
    conn = sqlite3.connect("charging_stations.db")
    df = read_from_db(conn)
    
    # Filter for offline stations only
    offline_df = df[df["is_offline"] == True]
    
    conn.close()
    
    return offline_df


# ===========================================
# TEST THE ALERT SYSTEM
# ===========================================
if __name__ == "__main__":
    # Run the health check
    check_offline_stations()
    
    # Show offline stations
    offline_stations = list_offline_stations()
    
    if len(offline_stations) > 0:
        print("\n📋 Offline stations:")
        print(offline_stations[["name", "city", "operator_clean", "days_since_update"]].to_string(index=False))
    else:
        print("\n✅ No offline stations found!")