# Rotten Tomatoes Movie Database System
## Project Overview
This project involves the development of a comprehensive SQL-based database system designed to manage and analyze a dataset of 17,712 films. The primary objective was to transition from a flat-file CSV structure to a fully normalized relational database using PostgreSQL and Python. The system allows for efficient data storage and features an interactive command-line interface for complex data analysis.

--------------------------------------------------------------------------------
## Project Sections & Applied Skills
### 1. Data Normalization & Schema Design
What was done: I performed a structural analysis of the original rotten_tomatoes_movies.csv to identify redundancies and multi-valued attributes. I then designed a 10-table normalized schema (including junction tables for actors, directors, and genres) to eliminate data duplication and ensure referential integrity.
**Data Skills Used**:

    Database Normalization: Applying 1NF, 2NF, and 3NF principles to transform flat data into relational entities.
    Data Definition Language (DDL): Writing structured SQL to define tables, primary keys, foreign keys, and constraints (NOT NULL, UNIQUE).
    PostgreSQL Dialect: Utilizing specific PostgreSQL features like SERIAL for auto-incrementing IDs.

### 2. Automated Data Population (db_load.py)
What was done: I developed a Python script to programmatically read the 17,712 film records and distribute them across the normalized SQL tables. This required handling "messy" data, such as converting empty strings to NULL and splitting comma-separated lists into individual relational rows.
**Data Skills Used**:

    Database Connectivity: Using psycopg2 to establish a secure bridge between Python and PostgreSQL.
    Data Cleaning & Transformation: Developing helper functions to sanitize strings and numeric values during the migration process.

### 3. Interactive Data Analysis Menu (eval03.py)
What was done: I implemented a robust menu-driven application providing 10 distinct analytical queries. Per project requirements, all calculations were performed strictly by SQL statements rather than Python, ensuring the database handled the heavy lifting.
Data Skills Used:

    Advanced SQL Querying: Writing complex queries involving multi-table JOINS, GROUP BY aggregations, and sorting.
    Date Arithmetic: Utilizing PostgreSQL INTERVAL functions to calculate streaming release windows.
    Window Functions: Implementing ROW_NUMBER() OVER(PARTITION BY...) to identify top-performing films per year.


--------------------------------------------------------------------------------
## How to Run

    Execute module03/create_database.sql in your PostgreSQL environment to build the schema.
    Run python module03/db_load.py to populate the 10 tables from the source CSV.
    Launch python module03/eval03.py to start the interactive analysis menu.


--------------------------------------------------------------------------------
## Repository Structure (main files)

    module03/create_database.sql: SQL schema definition.
    module03/db_load.py: Data migration and cleaning script.
    module03/eval03.py: Interactive analytical menu
