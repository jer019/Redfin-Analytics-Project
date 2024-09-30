# Redfin Analytics DAG Project

This project extracts, transforms, and loads (ETL) Redfin real estate data using Apache Airflow. The data is pulled from an external URL, transformed using Pandas, and finally uploaded to an Amazon S3 bucket.

## Project Overview

The DAG defined in this project performs the following steps:
1. **Extract**: Download Redfin real estate data from a compressed TSV file.
2. **Transform**: Clean and format the data, removing anomalies and extracting useful time information.
3. **Load**: Upload the transformed data as a CSV to an S3 bucket.

## Prerequisites

Before running this project, ensure you have the following installed:
- Apache Airflow (v2.5.0 or higher)
- Python 3.8+
- Amazon Web Services (AWS) credentials configured for S3 access
- The following Python libraries:
  - `pandas`
  - `boto3`

You can install the required packages using the `requirements.txt` file:

```bash
pip install -r requirements.txt
