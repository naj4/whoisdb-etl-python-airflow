# Python ETL Data Pipeline

This project is a Python-based ETL (Extract, Transform, Load) data pipeline designed to automate the process of extracting data from the Whois database, transforming it, and loading it into a PostgreSQL database. The entire pipeline is orchestrated using Apache Airflow and Docker.

## Features

- **Data Extraction**: Automatically extract data from the Whois database.
- **Data Transformation**: Clean, format, and transform the extracted data to meet the desired structure and standards.
- **Data Loading**: Load the transformed data into a PostgreSQL database.
- **Automation**: Use Apache Airflow to automate and schedule the ETL workflow.
- **Containerization**: Leverage Docker to containerize the ETL pipeline, ensuring consistency across different environments.

## Getting Started

### Prerequisites

- Docker
- Docker Compose

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/naj4/whoisdb-etl-python-airflow.git
   cd whoisdb-etl-python-airflow

2. **Set up and run the Docker containers:**:
   ```bash
   docker-compose up -d

3. **Access the Airflow web interface:**:
   Open your web browser and navigate to http://localhost:8080 to access the Airflow UI.

### Configuration

- **Database Configuration:** Ensure the PostgreSQL database details are correctly configured in the db_info.ini file located in the config directory.

- **Airflow DAGs:** The Airflow DAGs for the ETL process are defined in the dags directory.

### Usage

- **Trigger the ETL Pipeline:** Use the Airflow UI to trigger the ETL pipeline manually or set up a schedule for automatic execution.

- **Monitor the Pipeline:** Monitor the progress and status of the ETL pipeline using the Airflow UI.

## Project Structure
```arduino
├── config/
│   ├── db_info.ini
│   └── ...
├── dags/
│   ├── whois_etl.py
│   └── ...
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md

