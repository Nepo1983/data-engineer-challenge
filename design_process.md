# Design Process

1. **Schema Design**  
   - Identified fact (`fact_sales`) and key dimensions (`dim_product`, `dim_customer`, `dim_time`, `dim_currency_rate`).  
   - Ensured support for top-product and time-of-day analyses.

2. **Bronze–Silver–Gold Zones**  
   - **Bronze**: Raw extracts from source DBs & API as Parquet.  
   - **Silver**: Cleaned & joined data (denormalized fact & time dimensions).  
   - **Gold**: Loaded into DW and ready for reporting.

3. **Airflow DAG Structure**  
   - Parallel extraction tasks for orders, items, customers, products.  
   - Separate task to fetch currency rates.  
   - Single transform-and-load task for joins, calculations, and inserts.

4. **Idempotency & Partitioning**  
   - Filename includes processing date.  
   - Delete-insert approach in dimensional tables for each `sale_date` to avoid duplicates.

5. **Dashboard Planning**  
   - Designed visualizations to answer: top products and optimal promotion times.  
   - Included KPI tiles and interactivity (filters, toggles).

6. **Documentation & Testing**  
   - Provided `schema.sql`, DAG script, mockup, and this design doc.  
   - Recommend unit tests for each ETL function and integration tests in Airflow.

# Data Architecture

![DA](/images/Data_Architecture.png)
