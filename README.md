# 🔋 EV Charging Infrastructure ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An automated data pipeline that extracts, transforms, and loads electric vehicle charging station data for monitoring and analysis—mirroring Tesla's Supercharger network management workflow.

## 🎯 Project Overview

This project demonstrates end-to-end data engineering skills by building a production-style ETL (Extract, Transform, Load) pipeline that:

1. **Extracts** real-time charging station data from Open Charge Map API
2. **Transforms** raw data by cleaning, standardizing, and flagging offline stations
3. **Loads** processed data into a SQLite database for persistent storage
4. **Visualizes** network health through an interactive Streamlit dashboard
5. **Alerts** when >10% of the network goes offline

## 🛠️ Tech Stack

- **Python** - Core programming language
- **Pandas** - Data manipulation and analysis
- **SQLite** - Lightweight database for data storage
- **Streamlit** - Interactive web dashboard
- **Plotly** - Data visualization and mapping
- **Geopy** - GPS coordinate to city name conversion
- **Requests** - API data extraction

## 📁 Project Structure
ev-charging-pipeline/
├── etl_pipeline.py # Main ETL workflow (Extract → Transform → Load)
├── database.py # Database operations (Load + Query)
├── alerts.py # Health monitoring and alerting system
├── dashboard.py # Interactive web visualization
├── requirements.txt # Python dependencies
├── .gitignore # Files to exclude from Git
└── README.md # This file

## 🚀 How to Run

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/ev-charging-pipeline.git
cd ev-charging-pipeline
pip install -r requirements.txt
python etl_pipeline.py
streamlit run dashboard.py
python alerts.py
📊 Features
ETL Pipeline
Extracts charging station data from Open Charge Map API
Removes duplicates and standardizes operator names
Converts GPS coordinates to city names using reverse geocoding
Flags stations inactive for >90 days as "offline"
Database
Persistent storage using SQLite
Upsert logic to avoid duplicate entries
Query functions for dashboard integration
Dashboard
Real-time metrics (total stations, offline count, Tesla stations)
Interactive map showing station locations (color-coded by status)
Table of offline stations with details
Operator breakdown bar chart
Alerting System
Monitors offline percentage across the network
Triggers alert when >10% of stations are offline
Lists all offline stations for investigation
🎯 Tesla Relevance
This project directly mirrors the challenges Tesla faces in managing its global Supercharger network:
Multi-source data integration - Combining API data with geocoding services
Data reconciliation - Standardizing operator names and removing duplicates
Infrastructure monitoring - Proactive alerting for offline stations
Executive reporting - Dashboard for visualizing network health
Scalable architecture - Modular design ready for production deployment
👤 Author
Yeabsira Yirgu
Duke Kunshan University | Class of 2028
Applied Mathematics and Computer Science
