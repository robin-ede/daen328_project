# DAEN 328 Project

A data engineering project that implements an ETL pipeline and interactive dashboard for analyzing Chicago-related data.

## Project Overview

This project consists of three main components:
- **ETL Pipeline**: Extracts, transforms, and loads data into a PostgreSQL database
- **Dashboard**: A Streamlit-based web application for data visualization and analysis
- **Database**: PostgreSQL database for data storage

## Prerequisites

- Docker and Docker Compose
- Python 3.8+ (for local development)
- Git

## Setup Instructions

1. Clone the repository:
   ```bash
   git clone https://github.com/robin-ede/daen328_project
   cd daen328_project
   ```

2. Create a `.env` file in the root directory with the following variables:
   ```
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=postgres
   POSTGRES_DB=chicago_db
   POSTGRES_HOST=db
   POSTGRES_PORT=5432
   ```

3. Start the services using Docker Compose:
   ```bash
   docker-compose up --build
   ```

## Project Structure

```
daen328_project/
├── dashboard/           # Streamlit dashboard application
│   ├── Dockerfile
│   ├── requirements.txt
│   └── streamlit_app.py
├── etl/                # ETL pipeline
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── db/                 # Database initialization scripts
├── docker-compose.yml  # Docker configuration
└── README.md
```

## Accessing the Services

- **Dashboard**: http://localhost:8501
- **Database**: localhost:5433 (PostgreSQL)

## Development

### Local Development Setup

1. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r etl/requirements.txt
   pip install -r dashboard/requirements.txt
   ```

### Running Components Individually

- **ETL Pipeline**:
  ```bash
  cd etl
  python main.py
  ```

- **Dashboard**:
  ```bash
  cd dashboard
  streamlit run streamlit_app.py
  ```