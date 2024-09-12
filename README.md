# Sales ETL Pipeline

This project contains an ETL (Extract, Transform, Load) pipeline for processing sales data from JSON files and loading it into a PostgreSQL database.

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)

## Overview

The ETL pipeline extracts sales data from JSON files, transforms it into a structured format, and loads it into a PostgreSQL database. This process creates summary views of sales, facilitating analysis and reporting. The ETL pipeline is automated with the help of Windows Task Scheduler.

In addition to the sales and sales_detail tables, products and dates can be created and populated from the JSON files but i considered it unneccessary in this project.

The logging was used in the project to have an easy overview over scheduled extraction of data every day. 

Automated testing was implemented as well as a part of the project. Outrup from automated testing shows in the terminal. 

## Project Structure

```plaintext
etl_pipeline_w_tests_and_logs/
├── data/                                  # Directory containing JSON data files
├── tests/                                 # Directory containing automated test-scripts
│   ├── test_extract_data.py               # Python script for automated testing of extarct_data module
│   ├── test_transform_data.py             # Python script for automated testing of transform_data module
│   └── test_load_data.py                  # Python script for automated testing of load_data module
├── main.py                                # Main ETL script
├── extract_data.py                        # Script for extracting data from JSON-files
├── transform_data.py                      # Script for transforming data 
├── load_data.py                           # Script for loading data in Postgre database
├── .env                                   # Environment variables for database configuration
├── README.md                              # Project documentation
├── requirements.txt                       # Required packages
├── run_etl.bat                            # .bat file for scheduling ETL
├── etl_pipeline.log                       # Log-file for ETL runs
├── self_assessment.txt                     # Self-Assessment file
```

## Setup and Installation

1. **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/etl_converstionista.git
    cd etl_converstionista
    ```

2. **Create and activate a virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4. **Configure the database connection:**
    - Create an `.env` file in the root directory and add your PostgreSQL database configuration. You can also just change my file:
    ```
    PG_HOST=your_host
    PG_DATABASE=your_database
    PG_USER=your_username
    PG_PASSWORD=your_password
    ```

## Usage

Follow these steps to set up and run the ETL pipeline:

1. **Run SQL Scripts to Create Table sales**

   Execute the SQL script in order to create a table: 

   CREATE TABLE IF NOT EXISTS sales (
    ecommerce_transaction_id VARCHAR(255) PRIMARY KEY,
    event_date DATE,
    event_value_in_usd NUMERIC,
    user_pseudo_id VARCHAR(255),
    item_quantity NUMERIC,
    total_sales NUMERIC(10,2),
    total_sales_in_usd NUMERIC(10,2),
    FOREIGN KEY (event_date) REFERENCES dates(date)
);

2. **Execute the tests**
   To ensure that the code works correctly write in the terminal "pytest" and press Enter. Check that the tests are performed. Your expected output is a faluire at `test_load_data.py` that is caused by violation of unique-key constraint and no faluires at other tests. 

3. **Execute the main ETL pipeline script main.py**
   Run python main.py to extract sales data from JSON files, transform it into structured formats, and load it into the PostgreSQL database. Ensure your JSON data files are placed in the data/ directory before running the script.

   `main.py`

3. **Check the etl_pipeline.log to ensure that the script ran correctly**

   `etl_pipeline.log`

4. **Optional** 
   
   Run `main.py` one more time and check `etl_pipeline.log` to ensure that the fail of loading the same data occured. 