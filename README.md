# Data-Transformation-SparkSQL
Here's the updated README file that includes the `SilverZone-to-GoldZone.py` script:

```markdown
# Data Pipeline Scripts for Azure Data Lake and Databricks

This repository contains a set of Python scripts designed to implement a data pipeline using Azure Data Lake Storage (ADLS) and Databricks. The pipeline facilitates the transformation and movement of data through four zones: Landing Zone, Bronze Zone, Silver Zone, and Gold Zone.

## Overview

The scripts in this repository serve the following purposes:

1. **Mounting ADLS Using Service Principal**:
   - Establishes a connection to Azure Data Lake Storage using service principal credentials.
   - Mounts the ADLS storage to Databricks for seamless data access.

2. **LandingZone-to-BronzeZone**:
   - Reads raw data files from the Landing Zone in ADLS.
   - Applies basic transformations, such as adding a `process_date` column, and writes the data into the Bronze Zone in Parquet format.

3. **BronzeZone-to-SilverZone**:
   - Reads data from the Bronze Zone.
   - Applies cleaning, data type corrections, and NULL handling.
   - Writes the processed data into the Silver Zone in both ADLS and the Databricks Catalog (Delta tables).

4. **SilverZone-to-GoldZone**:
   - Reads data from the Silver Zone.
   - Performs advanced aggregations and calculations to generate insights.
   - Writes the resulting datasets into the Gold Zone for analytics and reporting.

## Scripts

### 1. Mounting ADLS Using Service Principal (`Mounting ADLS using service principal.py`)
- **Purpose**: Connects to Azure Data Lake Storage using service principal credentials and mounts the storage to Databricks.
- **Key Features**:
  - Uses OAuth for authentication.
  - Allows seamless access to ADLS files via the mounted path `/mnt/azure-bde-project`.

### 2. LandingZone-to-BronzeZone (`LandingZone-to-BronzeZone.py`)
- **Purpose**: Moves raw data from the Landing Zone to the Bronze Zone.
- **Key Features**:
  - Reads data from multiple CSV files (e.g., Athletes, Coaches, Medals).
  - Adds a `process_date` column to track processing time.
  - Writes the data to the Bronze Zone in partitioned Parquet format.

### 3. BronzeZone-to-SilverZone (`BronzeZone-to-SilverZone.py`)
- **Purpose**: Processes data from the Bronze Zone and writes cleaned and transformed data to the Silver Zone.
- **Key Features**:
  - Handles NULL values by applying default replacements based on column types.
  - Removes duplicates and adds `CreatedDate` and `ModifiedDate` columns.
  - Writes data to the Silver Zone in both ADLS (Delta format) and Databricks SQL tables.
  - Supports two load types: Full Load (FL) and Incremental Load (IL).

### 4. SilverZone-to-GoldZone (`SilverZone-to-GoldZone.py`)
- **Purpose**: Reads cleaned data from the Silver Zone and performs aggregations to create analytics-ready datasets in the Gold Zone.
- **Key Features**:
  - **Tokyo Olympics Dataset**:
    1. Identifies top countries with the highest number of gold medals.
    2. Calculates average gender entries for each discipline.
    3. Aggregates participant counts per country and discipline.
    4. Determines the number of disciplines and medals per country.
    5. Computes male and female participation in each discipline.
  - Writes processed datasets to the Gold Zone in ADLS and registers them as Delta tables in Databricks SQL Catalog.
  - Includes logic for specific transformations if used with other datasets (e.g., `GlobalAirlines`).

## Data Flow

1. **Landing Zone**: Contains raw data as CSV files.
2. **Bronze Zone**: Stores minimally transformed data in Parquet format.
3. **Silver Zone**: Contains cleaned and structured data, ready for analytics or further processing.
4. **Gold Zone**: Houses aggregated and curated data, optimized for reporting and advanced analytics.

## Configuration

The scripts rely on the following parameters:

- **Database Name (`dbName`)**: Specifies the name of the database being processed.
- **Load Type (`loadType`)**: Determines the load type (`FL` for Full Load, `IL` for Incremental Load).
- **Service Principal Credentials**:
  - `apporcientID`: Application (client) ID of the service principal.
  - `dirortenantID`: Directory (tenant) ID of the Azure AD tenant.
  - `clientsecret`: Client secret of the service principal.

## Prerequisites

- An Azure Data Lake Storage account.
- Databricks workspace with necessary permissions to access ADLS.
- Service principal credentials with access to the ADLS account.

## Usage

1. **Mount ADLS**:
   - Run the `Mounting ADLS using service principal.py` script to mount the ADLS storage.

2. **Process Data**:
   - Run the `LandingZone-to-BronzeZone.py` script to process raw data and store it in the Bronze Zone.
   - Run the `BronzeZone-to-SilverZone.py` script to clean and transform the data for the Silver Zone.
   - Run the `SilverZone-to-GoldZone.py` script to perform aggregations and generate analytics-ready datasets in the Gold Zone.

3. **Validate Outputs**:
   - Verify the processed data in the respective ADLS zones and Databricks SQL tables.

## Notes

- Ensure that sensitive credentials are stored securely using Databricks Secrets or environment variables.
- The pipeline is customizable to accommodate additional transformations or new data sources.
- The scripts are designed for the Tokyo Olympics dataset but can be adapted for other datasets.

## License

This project is licensed under the MIT License. Feel free to use and modify it for your own projects.
```

Let me know if you need further adjustments!
