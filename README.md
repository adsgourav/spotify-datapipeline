# Spotify End to End Data Pipeline

It's an ETL pipeline using the spotify API on AWS. The pipeline will retrieve data from the spotify API, transform it to a desired format, and load it into an AWS S3 bucket.

### Architecture
![Architecture Diagram](https://github.com/adsgourav/spotify-datapipeline/blob/main/Spotify-Pipeline-Archicture.png)

### About Dataset/API
This API contains information about music artist, albums and songs - [Spotify API](https://developer.spotify.com/documentation/web-api)

### Services Used
1. **S3 (Simple Storage Service):** Amazon Simple Storage Service (Amazon S3) is an object storage service that offers industry-leading scalability, data availability, security, and performance. You can use Amazon S3 to store and retrieve any amount of data at any time, from anywhere.

2.  **AWS Lambda:** AWS Lambda is a compute service that runs your code in response to events and automatically manages the compute resources, making it the fastest way to turn an idea into a modern, production, serverless applications.

3. **Cloud Watch:** Amazon CloudWatch is a monitoring and management service from Amazon Web Services (AWS). It collects and analyzes data from AWS resources to help users monitor and optimize their applications and infrastructure.

4. **Glue Crawler:** An AWS Glue crawler is a tool that automatically discovers, catalogs, and organizes data from various sources. It can scan data sources like Amazon S3, DynamoDB, JDBC, and MongoDB.

5. **Data Catalog:** The AWS Glue Data Catalog is a centralized repository for storing metadata about data assets in the AWS Cloud. It's a managed service that helps users discover, prepare, and integrate data from multiple sources.

6. **AWS Athena:** Amazon Athena is a serverless, interactive query service that allows users to analyze data in Amazon Simple Storage Service (S3). It uses standard SQL to process structured, semi-structured, and unstructured data.

### Installed packages
```
pip install pandas
pip install numpy
pip install spotify
```

### Project Execution Flow
Extract Data from API -> Lambda Trigger (every 1 hour) -> Run Extract Code -> Store Raw Data in S3 -> Trigger Transformation Function -> Transform data and load it in S3 -> Query using Athena
