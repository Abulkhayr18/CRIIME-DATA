Crime Data ETL Project
Overview
This project implements a comprehensive ETL (Extract, Transform, Load) pipeline for processing UK crime statistics data. The pipeline transforms raw crime data from CSV format into a structured SQL Server database with enhanced analytical capabilities including temporal analysis, crime categorization, and statistical summaries.
Features

Data Ingestion: Automated bulk loading of crime data from CSV files
Data Quality Analysis: Comprehensive data profiling and validation
Temporal Enhancement: Financial year and quarter date calculations
Data Standardization: Consistent formatting and data type optimization
Crime Classification: Enhanced categorization for analytical purposes
Statistical Views: Pre-built views for quarterly crime analysis

Database Schema
Main Table: CRIME_DATA
The primary table contains the following key fields:
Original Data Fields

FINANCIAL_YEAR (VARCHAR): Financial reporting year
FINANCIAL_QUARTER (VARCHAR): Quarter within financial year (Q1-Q4)
FORCE_NAME (VARCHAR): Police force/department name
OFFENCE_DESCRIPTION (VARCHAR): Detailed crime description
OFFENCE_GROUP (VARCHAR): High-level crime category
OFFENCE_SUBGROUP (VARCHAR): Subcategory of crime
OFFENCE_CODE (VARCHAR): Unique crime classification code
NUMBER_OF_OFFENCES (VARCHAR): Primary offense count
NUMBER_OF_OFFENCES2 (VARCHAR): Secondary offense count

Enhanced Fields (Added During ETL)

TOTAL_OFFENCES (VARCHAR): Calculated total combining both offense counts
FINANCIAL_YEAR_START_DATE (DATE): Start date of financial year
FINANCIAL_YEAR_END_DATE (DATE): End date of financial year
CALENDAR_YEAR (VARCHAR): Extracted calendar year
CALENDAR_QUARTER (INT): Quarter number (1-4)
QUARTER_START_DATE (DATE): Quarter start date
QUARTER_END_DATE (DATE): Quarter end date
FORCE_CATEGORY (NVARCHAR): Police force categorization
CRIME_CATEGORY_MAJOR (NVARCHAR): Major crime classification
IS_VIOLENT_CRIME (BIT): Boolean indicator for violent crimes
IS_PROPERTY_CRIME (BIT): Boolean indicator for property crimes
OFFENCES_PER_100k_POPULATION (DECIMAL): Population-adjusted crime rate

Prerequisites

SQL Server (2016 or later recommended)
CSV file containing crime data with proper headers
Appropriate permissions for:

Database creation and modification
Bulk insert operations
File system access for CSV import



Installation & Setup
1. Database Setup
sql-- The database will be created and renamed during the ETL process
-- Initial database: FINANCIAL_AUDIT
-- Final database: CRIME_RECORD
2. Data File Preparation

Place your crime data CSV file at: C:\Program Files\Microsoft SQL Server\CRIME_DATA.csv
Ensure the CSV has proper headers matching the expected schema
Verify comma-separated format with newline row terminators

3. Execute ETL Pipeline
Run the SQL scripts in the following order:

Database creation and table setup
Data type modifications and optimizations
Bulk data import
Data quality analysis
Data transformation and enhancement
View creation for analytics

ETL Process Flow
1. Extract Phase

Bulk Insert: Loads CSV data using SQL Server's BULK INSERT command
Data Validation: Initial data quality checks and profiling

2. Transform Phase

Data Type Optimization: Converts and standardizes column types
Data Cleaning: Removes commas from numeric fields, handles null values
Calculated Fields: Creates TOTAL_OFFENCES by combining offense counts
Temporal Processing:

Calculates financial year start/end dates
Determines calendar quarters and date ranges
Handles both single year and year-range formats



3. Load Phase

Enhanced Schema: Adds analytical columns for deeper insights
Data Categorization: Prepares framework for crime type classification
View Creation: Builds analytical views for reporting

Key Transformations
Financial Year Processing
The ETL handles two financial year formats:

Single year format: 2023
Year range format: 2023/24

Financial years run from April 1st to March 31st of the following year.
Offense Count Consolidation
sqlTOTAL_OFFENCES = NUMBER_OF_OFFENCES + NUMBER_OF_OFFENCES2
Handles missing values and comma-separated numbers.
Quarterly Date Calculations

Q1: April - June
Q2: July - September
Q3: October - December
Q4: January - March

Data Quality Features
Validation Queries
The project includes comprehensive data quality checks:

Record counts by police force
Distinct offense types analysis
Missing data identification
String length validation
Cross-column comparison analysis

Sample Quality Check
sql-- Compare offense count columns for data integrity
SELECT 
    comparison_type,
    record_count
FROM (
    -- Analysis of NUMBER_OF_OFFENCES vs NUMBER_OF_OFFENCES2
    -- Results show data consistency patterns
) comparison_results;
Analytics Views
Quarterly Crime Review
sqlCREATE VIEW V_QUARTERLY_CRIME_REVIEW AS
SELECT 
    Calendar_Year,
    Financial_Quarter,
    COUNT(*) as Crime_Types_Count,
    SUM(Total_Offences) as Total_Offences,
    SUM(CASE WHEN Is_Violent_Crime = 1 THEN Total_Offences ELSE 0 END) as Violent_Crime_Count,
    SUM(CASE WHEN Is_Property_Crime = 1 THEN Total_Offences ELSE 0 END) as Property_Crime_Count
FROM CRIME_DATA
WHERE Total_Offences > 0
GROUP BY Calendar_Year, Financial_Quarter, Force_Category, Crime_Category_Major;
Usage Examples
Basic Crime Statistics
sql-- Get total offenses by police force
SELECT 
    FORCE_NAME,
    SUM(CAST(TOTAL_OFFENCES AS INT)) as Total_Crimes
FROM CRIME_DATA
GROUP BY FORCE_NAME
ORDER BY Total_Crimes DESC;
Temporal Analysis
sql-- Quarterly crime trends
SELECT 
    Calendar_Year,
    Calendar_Quarter,
    SUM(CAST(TOTAL_OFFENCES AS INT)) as Quarterly_Total
FROM CRIME_DATA
GROUP BY Calendar_Year, Calendar_Quarter
ORDER BY Calendar_Year, Calendar_Quarter;
Performance Considerations

Indexes should be added on frequently queried columns (FORCE_NAME, Calendar_Year, OFFENCE_GROUP)
Consider partitioning large datasets by year for improved performance
Regular statistics updates recommended for optimal query performance

File Structure
crime-data-etl/
├── README.md
├── sql/
│   ├── 01_database_setup.sql
│   ├── 02_table_creation.sql
│   ├── 03_data_import.sql
│   ├── 04_data_transformation.sql
│   ├── 05_quality_checks.sql
│   └── 06_views_creation.sql
├── data/
│   └── sample_crime_data.csv
└── docs/
    ├── schema_diagram.png
    └── data_dictionary.md
Data Sources
This ETL pipeline is designed to work with UK crime statistics data, typically sourced from:

Police force databases
Government crime statistics portals
Public safety data repositories
