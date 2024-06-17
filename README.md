# Hospital Admissions Data Pipeline

## Overview
This project aims to create a data pipeline for hospital admission-related data. The pipeline will load, clean, transform, and store the data in an efficient manner. Additionally, it will create data models in the data warehouse to facilitate AI model development for predicting factors affecting hospital admissions by data scientists or AI engineers.

![Pipeline image](/health_pipeline.png)


## Project Steps

1. **Data Ingestion:**
   - Load hospital admission data from the source (Admission Data.csv, Mortality Data.csv and Pollution Data.csv).
   - Transform the raw data into a structured format.

2. **Data Cleaning and Transformation:**
   - Clean and preprocess the data (handle missing values, outliers, etc.).
   - Perform necessary transformations .

3. **Storage:**
   - Store the cleaned data in an S3 bucket for easy access.

4. **Data Warehousing:**
   - Create Redshift tables to organize the data.
   - Define the appropriate schema (e.g., fact tables for admissions, dimension tables for related information).

5. **Star Schema Modeling:**
   - Design star schema models to simplify querying and analysis.
   - Identify relevant dimensions and facts tables.


## Running the Pipeline
1. Install dependencies by running:
   ```
   pip install -r requirements.txt
   ```

2. Execute the Jupyter Notebook (`etl_script.ipynb`) by running each code block sequentially.

3. Create the required tables needed to recieve the data in Redshift using the tables.sql file

4. Create the models and their connections using the models.sql file. 



# Star Schema for Hospital Admissions Analysis

## Overview
The star schema is designed to facilitate advanced analytics on hospital admissions, patient outcomes, and environmental factors. It transforms raw data from three sources—admissions, mortalities, and pollution levels—into a structured, analytics-ready format. This design optimizes for query performance, data integrity, and ease of use for data analysts and AI engineers.

## Key Components

### Fact Table: `fact_hospital_events`
- **Purpose**: Records individual hospital events, either admissions or mortalities.
- **Granularity**: One row per event.
- **Key Fields**:
  - `event_id` (PK): Unique identifier for each event.
  - `event_type`: 'ADMISSION' or 'MORTALITY'.
  - `event_date` (FK): Links to `dim_date`, enabling time-based analysis.
  - `patient_id` (FK): Links to `dim_patient`, for patient-centric analysis.
  - `admission_id` (FK): Links to `dim_admission`, for admission-specific details.

### Dimension Tables

1. **`dim_date`**
   - **Purpose**: Enables time-based analysis and links events to pollution data.
   - **Key Fields**: `date_id` (PK), `year`, `month`, `day`, `day_of_week`, `is_weekend`, `quarter`.
   - **Note**: Acts as a bridge between `fact_hospital_events` and `dim_pollution`.

2. **`dim_patient`**
   - **Purpose**: Stores patient demographics and health history.
   - **Key Fields**: `patient_id` (PK), `age`, `gender`, `rural`, `smoking`, `alcohol`, `dm` (diabetes), `htn` (hypertension), etc.
   - **Note**: Contains attributes that are consistent across a patient's multiple visits.

3. **`dim_admission`**
   - **Purpose**: Holds details specific to each hospital admission.
   - **Key Fields**: `admission_id` (PK), `patient_id` (FK to `dim_patient`), `duration_of_stay`, `outcome`, `heart_failure`, `acs` (acute coronary syndrome), etc.
   - **Note**: Separating from `dim_patient` normalizes data and allows tracking changes across admissions.

4. **`dim_pollution`**
   - **Purpose**: Tracks daily environmental conditions that might impact health.
   - **Key Fields**: `pollution_date` (PK, FK to `dim_date`), `aqi`, `pm_avg`, `ozone_avg`, `max_temp`, etc.
   - **Note**: Linked to events via `dim_date`, enabling correlation of health events with environmental factors.

## Key Design Decisions

1. **Event-Centric Fact Table**: By making `fact_hospital_events` event-centric rather than just admission-centric, we can analyze both admissions and mortalities in a unified way.

2. **Separating Patient and Admission Data**: This separation allows us to track patient history across visits and analyze how pre-existing conditions affect outcomes.

3. **Date Dimension as a Bridge**: Using `dim_date` to link events and pollution data allows us to analyze health outcomes in the context of daily environmental conditions without denormalizing pollution data into the fact table.

4. **Flexibility in Data Types**: Using `VARCHAR` for many fields (in `dim_admission` and `dim_pollution`) allows for flexibility in data representation, especially for fields that might have non-numeric values or require more investigation.

## Querying Capabilities

This schema facilitates a wide range of analytical queries, such as:

1. "How do admission rates for respiratory issues correlate with high PM2.5 days?"
2. "Do patients with a history of hypertension have longer stays during heatwaves?"
3. "Is there a seasonal pattern to heart failure admissions, and does it correlate with pollution levels?"
4. "Do rural patients with diabetes have different outcomes compared to urban patients?"

## Implementation Notes

- **Redshift Considerations**: Due to Redshift limitations on `CHECK` constraints, we use a view `vw_fact_hospital_events` to enforce the `event_type` constraint.
- **Data Quality**: The ETL process (in `time_format` function) flags invalid dates, ensuring data quality without losing records.
- **Scalability**: The star schema's denormalized nature and optimized join patterns make it suitable for large-scale data and complex queries.

## Future Enhancements

1. **Geospatial Data**: Add a `dim_location` to analyze pollution effects by area.
2. **Treatment Dimension**: Track treatments given during each admission.
3. **Temporal Analysis**: Enhance `dim_date` with fiscal periods, seasons, or event markers (e.g., public health campaigns).
4. **Slowly Changing Dimensions**: Implement SCD techniques in `dim_patient` to track changes in patient attributes over time.

## Conclusion

This star schema transforms complex, interrelated healthcare and environmental data into a format optimized for analytical insights. It balances data integrity, query performance, and ease of use, making it an excellent foundation for AI-driven analysis of hospital admissions and public health.

## Notes
- Ensure that you have the necessary AWS credentials for S3 and Redshift.
- Explanations for the source data columns is in the table_headings.csv
- Customize the pipeline to match your specific data sources and requirements.
