# Databricks on AWS - Demos
![CLOUD](https://img.shields.io/badge/CLOUD-AWS-orange?logo=amazonaws&style=for-the-badge)
[![LAB](https://img.shields.io/badge/Lab-1_days-green?style=for-the-badge)](https://databricks.com/try-databricks)
## Description

This repository contains a collection of demos illustrating the integration of Databricks with various AWS native services, focusing on data ingestion, transformation, and serving.

## Table of Contents
1. [Databricks Intro](#databricks-intro)
2. [Ingestion](#ingestion)
3. [Data Transformation/Enrichment](#data-transformation)
4. [Data Serving/Consumption](#data-serving)
5. [Prerequisites](#prerequisites)
6. [Setup and Running Demos](#setup)
7. [Contributing](#contributing)
8. [License](#license)
9. [Contact](#contact)

<a name="databricks-intro"></a>
## Databricks Intro
- What is Databricks
- Databricks on AWS - networking considerations

<a name="ingestion"></a>
## Ingestion
- Ingesting data from S3 with Autoloader (Directory Listing and File Notification)
- Ingesting data from a Kinesis
    - Structured Streaming
    - Kinesis Firehose + Autoloader
- Ingesting data from an RDS Database
    - Bulk load using AWS Database Migration Service (DMS)
    - CDC using AWS Database Migration Service (DMS)

<a name="data-transformation"></a>
## Data Transformation/Enrichment
- Building an ETL Pipeline using Delta Live Tables
- Building a data pipeline using Databricks Workflows
- Reduce TCO by using AWS Graviton

<a name="data-serving"></a>
## Data Serving/Consumption
- Query your Delta Lake using Amazon Athena 
- Pushing Gold data to DynamoDB for low latency use cases
- Real-Time ML Inference using Sagemaker Serverless Endpoints
- Real-Time ML Inference using Databricks Model Serving V2
- Visualization using Amazon QuickSight

<a name="prerequisites"></a>
## Prerequisites

To run these demos, you will need:
- An AWS account with necessary permissions to create and manage resources
- A Databricks account
- Basic knowledge of AWS services and Databricks

<a name="setup"></a>
## Setup and Running Demos
1. Clone this repository to your local machine.
2. Set up your AWS and Databricks credentials.
3. Follow the individual READMEs in each demo's folder to set up and run the demos.

<a name="contributing"></a>
## Contributing
We welcome contributions to this project. Please refer to the CONTRIBUTING.md file for more details.

<a name="license"></a>
## License

&copy; 2023 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  

<a name="contact"></a>
## Contact
For any questions or feedback, please open an issue on this GitHub repository.

---

We hope you find these demos useful as you explore the capabilities of Databricks on AWS! Happy data engineering!

## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
