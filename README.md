# Sales ETL Pipeline

This project contains an ETL (Extract, Transform, Load) pipeline for processing sales data from JSON files and loading it into a PostgreSQL database.

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Database Schema](database-schema)
- [Visuals](#visuals)
- [Potential Database Schema](#potential-database-schema)

## Overview

The ETL pipeline extracts sales data from JSON files, transforms it into a structured format, and loads it into a PostgreSQL database. This process creates both summary and detailed views of sales, facilitating analysis and reporting. The ETL pipeline is automated with the help of Windows Task Scheduler.

In addition to the sales and sales_detail tables, products and dates tables were created and populated from the JSON files.

In the db_design_w_queries directory, you can find a .png file illustrating the potential database structure with more time and information. Queries for creating these tables are also provided in the same directory.


## Project Structure

```plaintext
conversionista_etl/
├── data/                                  # Directory containing JSON data files
├── possible_db_design/                    # Directory containing possible db-design and queries to create tables
│   ├── create_tables.sql                  # SQL script to create the necessary tables accorging to design
│   └── db_diagram.png                     # Possible database schema diagram
├── current_db_design/                     # Directory containing queries to create tables for this project and a query for populating dates-table
│   ├── current_db_design.png              # Current database schema diagram
│   ├── create_table_queries.sql           # SQL script to create the necessary tables for just this project
│   └── populating_dates_table_query.sql   # Query for populating dates-table in this project
├── visuals/                               # Directory containing visuals and script for them
│   ├── most_sold_categories.png           # Pie chart for most sold categories
│   ├── sales_per_date.png                 # Bar chart sales-per-date
│   └── visuals.py                         # Script for creating visuals
├── etl_sales_pipeline.py                  # Main ETL script
├── populating_products.py                 # Script for populating products-table from json-files
├── visual.py                              # Script for creating a few visuals of sales-data
├── .env                                   # Environment variables for database configuration
├── README.md                              # Project documentation
├── requirements.txt                       # Required packages
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

1. **Run SQL Scripts to Create Tables**

   Execute the SQL scripts located in the `sql_queries/` directory to create the necessary tables in your PostgreSQL database. Ensure you have connected to your database before running these scripts.

   `current_db_design/create_tables.sql`

2. **Run SQL Script for Populating Dates Table**

   Run the SQL script to populate the dates table with required data.
   
   `current_db_design/populating_dates_table_query.sql`

3. **Run Python Script for Populating Products Table**

   Execute the Python script `populating_products.py` located in the root directory to populate the products table from JSON data files in the data/ directory.

   `python populating_products.py`


4. **Execute the main ETL pipeline script etl_sales_pipeline.py**
   Run python etl_sales_pipeline.py to extract sales data from JSON files, transform it into structured formats, and load it into the PostgreSQL database. Ensure your JSON data files are placed in the data/ directory before running the script.

   `python etl_sales_pipeline.py`

## Database Schema

Current database schema: 

![Current Database Structure](current_db_design/current_db_design.png)

## Visuals

![Sales per date](visuals/sales_per_date.png)

![Most sold categories](visuals/most_sold_categories.png)

## Potential Database Schema

Database structure with more time and data could look like this:

![Possible Database Structure](possible_db_design/db_design.png)

