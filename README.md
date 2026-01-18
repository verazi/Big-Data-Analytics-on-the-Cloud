# COMP90024 Team 4 Cloud Application

## Video Link: https://www.youtube.com/watch?v=GU4w4_OKUO0

## Project Overview
This project collects, analyzes, and visualizes data from Reddit and Mastodon platforms, using ElasticSearch/Kibana as the data storage and query backend. It supports both local development testing and deployment to Fission cloud functions.

## Repository Structure

```
comp90024_team_4/
├── docker-compose.yml          # ElasticSearch and Kibana services configuration
├── README.md                   # Project documentation
├── backend/                    # Backend data processing and harvesting
│   ├── requirements.txt        # Python dependencies for backend
│   ├── shared-data-reddit.yaml # Reddit configuration for Kubernetes
│   ├── shared-data.yaml        # Mastodon and general configuration for Kubernetes
│   ├── fission/               # Fission cloud function implementations
│   │   ├── mastodon/          # Real-time Mastodon harvester (from recent onwards)
│   │   │   ├── build.sh       # Build script for Fission package
│   │   │   ├── requirements.txt
│   │   │   ├── test.py        # Main Mastodon harvester implementation
│   │   │   └── test1.py       # Additional harvester script
│   │   ├── housing.zip        # Packaged housing-related functions
│   │   └── mastodon.zip       # Packaged Mastodon functions
│   ├── harvesters/            # Local development harvesters
│   │   ├── mastodon_harvester.py  # Local test script for Mastodon API data collection
│   │   └── reddit_harvester.py    # Local test script for Reddit API data collection
│   ├── history-mastodon/      # Historical Mastodon data backfill (2019-2024)
│   │   ├── Dockerfile
│   │   ├── historical_harvester.py    # Historical data harvester
│   │   ├── history-mastodon-afford.yaml   # Affordability topic configuration
│   │   ├── history-mastodon-housing.yaml  # Housing topic configuration
│   │   ├── history-mastodon-mortgage.yaml # Mortgage topic configuration
│   │   ├── history-mastodon-rent.yaml     # Rent topic configuration
│   │   └── requirements.txt
│   ├── processor/             # Data processing and ElasticSearch upload
│   │   ├── bulk_insert_to_es.py   # Bulk data insertion to ElasticSearch
│   │   ├── main.py                # Main data processing pipeline
│   │   └── upload_to_es.py        # ElasticSearch upload utilities
│   ├── reddit/                # Reddit Fission + CronJob workflow (backward scraping)
│   │   ├── build.sh
│   │   ├── Dockerfile
│   │   ├── reddit_harvester.py             # Main Reddit harvester implementation
│   │   ├── reddit-harvester-function.py    # Fission function entry point
│   │   ├── reddit-harvester-historical.zip # Packaged historical harvester
│   │   ├── requirements.txt
│   │   ├── task_producer.py                # Task producer for CronJob workflow
│   │   └── task-producer-cronjob.yaml      # CronJob configuration
│   ├── utils/                 # Utility scripts and data conversion tools
│   │   ├── data_stat.py       # Data statistics utilities
│   │   ├── JSONConversion.py  # JSON format conversion
│   │   ├── NDJSONSplitter.py  # NDJSON file splitting utility
│   │   ├── PushiftConversion.py # Pushshift data conversion
│   │   └── *.json/*.ndjson    # Sample data files
│   └── visualization/         # Data visualization scripts
│       └── analysis.py        # Analysis and visualization tools
├── data/                      # Raw data storage (local testing data from API)
│   ├── mastodon/             # Mastodon platform test data
│   │   └── AU-ACT/           # Australia Capital Territory data
│   │       ├── comments.json
│   │       └── posts.json
│   └── reddit/               # Reddit platform test data
│       └── AU-ACT/           # Australia Capital Territory data
│           ├── comments.json
│           └── posts.json
├── database/                 # Database processing and analysis
│   ├── comments.json         # Processed comments data
│   ├── posts.json           # Processed posts data
│   └── fission/             # Fission-based analysis functions
│       ├── analysis.py      # Sentiment analysis and data processing
│       ├── analysis.zip     # Packaged analysis functions
│       ├── build.sh
│       └── requirements.txt
└── frontend/                # API and visualization frontend
    ├── api.ipynb           # API documentation and examples
    ├── api.py              # FastAPI backend implementation
    ├── requirements.txt    # Python dependencies for frontend
    ├── visualisation.ipynb # Visualization notebooks
    ├── visualisation.py    # Visualization scripts
    ├── clients-server/     # Client-server implementations
    │   ├── finalserver.ipynb  # Final server notebook for analysis
    │   └── server.ipynb       # Server implementation notebook
    └── fission/            # Fission-based frontend functions
        ├── Projection/     # Data projection functions
        │   ├── build.sh
        │   ├── projection.py
        │   └── requirements.txt
        └── Query/          # Query functions for data retrieval
            ├── build.sh
            ├── query.py    # Main query implementation
            ├── query.zip   # Packaged query functions
            └── requirements.txt
```

### Key Components

- **Backend**: Multi-strategy data harvesting with different temporal approaches:
  - **Real-time Mastodon**: `fission/mastodon/` - Continuous harvesting from recent data onwards
  - **Historical Mastodon**: `history-mastodon/` - Backfill historical data (2019-2024)
  - **Backward Reddit**: `reddit/` - Fission + CronJob workflow for backward scraping 
- **Frontend**: API services, data visualization, and interactive analysis notebooks
- **Database**: Sentiment analysis, data processing, and storage management
- **Data**: Local testing data from API calls, organized by platform and region 
- **Fission Functions**: Cloud-native serverless functions for scalable deployment
- **Docker Configuration**: Local development environment with ElasticSearch and Kibana


## Installation and Setup

### Cloud Deployment
For production deployment on Kubernetes with Fission:

1. **Setup Fission Environment**
   ```bash
   fission env create --name python39 --image fission/python-env-3.9 --builder fission/python-builder-3.9 --externalnetwork=true
   ```

2. **Deploy ConfigMaps**
   ```bash
   kubectl apply -f backend/shared-data.yaml
   kubectl apply -f backend/shared-data-reddit.yaml
   ```

3. **Deploy Functions**
   - Package and deploy harvester functions from `backend/fission/`, `backend/reddit/`
   - Deploy analysis functions from `database/fission/`
   - Deploy query APIs from `frontend/fission/Query/`

## How to Use the Client

### Local API Client
1. **Start the FastAPI server:**
   ```bash
   uvicorn frontend.api:app --host 0.0.0.0 --port 8000
   ```

2. **Access API endpoints:**
   - API Documentation: http://localhost:8000/docs
   - Interactive examples: Open `frontend/api.ipynb`

### Jupyter Notebook Analysis
1. **Start Jupyter Lab:**
   ```bash
   jupyter lab
   ```

2. **Open analysis notebooks:**
   - Main analysis: `frontend/clients-server/finalserver.ipynb`
   - Visualization: `frontend/visualisation.ipynb`
   - API examples: `frontend/api.ipynb`

### Cloud API Client (Production)
Access deployed Fission functions via HTTP endpoints:

```bash
# Example API calls (replace <router-service> with your Fission router URL)
curl "http://<router-service>:9090/api/posts-per-day?start_days=7&end_days=0"
curl "http://<router-service>:9090/api/sentiment-dist?start_days=30&end_days=0"
```

### Data Query Examples
```bash
# Query ElasticSearch directly
curl -X GET "http://localhost:9200/housing_comments/_search?size=3&pretty"

# Upload test data
python backend/processor/bulk_insert_to_es.py

# Run data analysis
python backend/visualization/analysis.py
```

## Available API Endpoints

### Query Functions
- **Posts per Day**: `/api/posts-per-day?start_days=X&end_days=Y`
- **Sentiment Distribution**: `/api/sentiment-dist?start_days=X&end_days=Y`  
- **Word Cloud Data**: `/api/wordcloud?start_days=X&end_days=Y`
- **Content Data**: `/api/content?start_days=X&end_days=Y`

### Parameters
- `start_days`: Number of days ago to start query (default: 7)
- `end_days`: Number of days ago to end query (default: 0)
