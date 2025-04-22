# DataEngineering

## Overview
This project demonstrates an end-to-end data engineering pipeline using **Azure**, **Databricks**, **Python**, **SQL**, and **Power BI**. It processes a healthcare dataset to clean, transform, and analyze patient data, culminating in an interactive Power BI dashboard for insights. The pipeline showcases core data engineering skills, including data ingestion, transformation, storage, and visualization.

## Dataset
The project uses the Healthcare Dataset from Kaggle, a synthetic dataset designed for multi-category classification, containing patient demographics, diagnoses, and treatment details.

## Pipeline Architecture
- **Ingestion**: Data is ingested from a CSV file stored in **Azure Blob Storage** using **Azure Data Factory**.
- **Processing**: **Databricks** with **Python** and **Spark SQL** is used to clean data (e.g., handling missing values, encoding categories) and transform it into a structured format.
- **Storage**: Processed data is stored in **Parquet** format in Azure Data Lake Storage.
- **Analysis & Visualization**: **Power BI** connects to the processed data to create an interactive dashboard displaying key metrics (e.g., patient age distribution, diagnosis trends).

## Tools & Technologies
- **Azure**: Blob Storage, Data Factory, Data Lake Storage
- **Databricks**: Python, Spark, SQL
- **Power BI**: Data visualization and dashboard creation
- **Other**: Git, Kaggle


## Results
- Cleaned and transformed a 10,000+ row healthcare dataset.
- Built a Power BI dashboard visualizing patient demographics and diagnosis patterns.
- Demonstrated scalable cloud-based data engineering with Azure and Databricks.
