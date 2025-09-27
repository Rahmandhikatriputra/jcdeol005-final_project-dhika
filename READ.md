# Data Visualization Implementation to Comprehend ANZ Bank Customer Behavior Based on Banking Transaction

## 1. Background
### 1.1 Case Project
![ANZ Bank](./documentation/Picture1.png)

ANZ Bank is a private Australian bank with thousands of customers. In this project, ANZ Bank had a dataset in the form of a table that recorded daily customer transactions. They wanted to learn more about customer behavior while using ANZ services. Furthermore, they wanted this information to be accessible at any time, without requiring a time limit on request.
### 1.2 Solution
To address this issue, it is necessary to implement a system that can migrate and transform raw data (in this case, ANZ bank customer transaction data) into a platform that can then be accessed online at any time. In this case, a data engineer can create a data pipeline program as a solution.

![data engineer](.\documentation\Picture2.png)

A data engineer is someone who designs and builds a system to manage and process data, which is then used by relevant users for further analysis. One such system is a data pipeline program, which aims to **extract** data from the source, **transforms** it, and **loads** the processed data in a database, which will then be used by the end user.
For this case, **batch processing** concept will be used to solve this problem. In batch processing, data production is done in batches with a time lag between each batch. Examples: daily retail sales reports, monthly electricity bill reports, etc. 

## 2. Tools Stack
In order to build the data pipeline program, several tools will be utilized. There are:

### 2.1 Programming Language
Programming language used to build data pipeline programs. In this project we use **Python** and **SQL** as the main programming language
### 2.2 Code Editor
To build and manage code structures in data pipeline programs. **Microsoft's Visual Studio Code** will be the preferred code editor tool for this project
### 2.3 Cloud Storage
An online digital data storage system, used to store data during the data pipeline process. **bigquery**, utilized as a data warehouse, stores structured data in a form of spreadsheet or table format. Meanwhile, **Google Cloud Storage**, utlized as data lake, stores semi-structured or unstructured data or various file formats, e.g. csv files, photos, etc.
### 2.4 Task Orchestration
Used to manage the execution flow of the code being built. Manage here includes scheduling and the flow of the code execution scheme. **Apache Airflow** will be used for this task
### 2.5. Data Processing
Used to process data in a data pipeline program. For this job, we use the **Data Build Tool (DBT)** for transformation work
### 2.6 Tool / Application Deployment
To build and run tools in isolation (not run on the local machine) so that tools can be built with packages and dependencies according to needs. **Docker** should be the suitable tool for this job
### 2.7 Version Control
To record work history during the program development process. For this, we use **Github**

## 3. Data Flow Scenario
There are at least 2 main scenario in this case:

![data flow scenario](.\documentation\2025-09-26_JCDEOL005_FinalProject_Dhika.png)

Data production: Transaction activities that generate data, then the resulting data is stored in the data lake.
Data pipeline: Data sources from the data lake are loaded into the data warehouse. Within the data warehouse, the data goes through three transformation stages:
-  Raw -> staging: A minimal transformation is performed, including changing the data type for all columns (all data in the raw table is initially string, but the data type is changed according to the data context, e.g., the date column is changed to date, the nominal column is changed to integer, etc.).
- Staging -> model: A major transformation is performed, including splitting the dataset into a fact table and several supporting tables (table dimensions) according to the case study.
- Model -> Mart: The transformation process produces tables ready for end-user use. E.g., monthly sales recaps, customer locations where transactions have been made, etc.

After all tranformation processes have been done up until mart tables within the data warehouse, all mart tables are visualized to facilitate the interpretation of the resulting data.

## 4. Workflow
There are at least 3 main workflows in this project:
- Initiation and Building Tools: Build the tools that will be used in the final project. Only two tools need to be built: Airflow using Docker and the Data Build Tool (DBT) by initiating dbt in a single folder.
- Dummy Data Production: Create a program to create a dummy dataset as a data source. The tools used are Python and its packages, and are executed using Airflow as a task orchestration.
- Data Pipeline Program Creation: Create an end-to-end data pipeline program from extracting data from the data lake, loading it into the data warehouse, and transforming it to the data mart stage, where it is then used by end users for further analysis.

## 5. Dataset Structure
This section will explain the form and structure of the dataset used and created. For the dataset structure, only the **raw table**, **model table**, and **mart table** will be explained.

### 5.1 Raw Table
| No | Nama Kolom        | Tipe Data | Tipe Key Data |
| -- | ----------------- | --------- | ------------- |
| 1  | status            | String    |               |
| 2  | card_present_flag | Integer   |               |
| 3  | bpay_biller_code  | Integer   |               |
| 4  | account           | String    |               |
| 5  | currency          | String    |               |
| 6  | long_lat          | String    |               |
| 7  | txn_description   | String    |               |
| 8  | merchant_id       | String    | Foreign Key   |
| 9  | merchant_code     | Integer   |               |
| 10 | first_name        | String    |               |
| 11 | date              | Date      |               |
| 12 | gender            | String    |               |
| 13 | age               | Integer   |               |
| 14 | merchant_suburb   | String    |               |
| 15 | merchant_state    | String    |               |
| 16 | extraction        | Datetime  |               |
| 17 | amount            | Boolean   |               |
| 18 | transaction_id    | String    | Primary Key   |
| 19 | country           | String    |               |
| 20 | customer_id       | String    | Foreign Key   |
| 21 | merchant_long_lat | String    |               |
| 22 | movement          | String    |               |

The data source used comes from ANZ Bank transaction data, a private Australian bank. The data records every transaction (money in and out) from a customer's account to a merchant, and also stores supporting data for each transaction, both for the customer and the merchant.

### 5.2 Model Table
![data flow scenario](.\documentation\Table_model.jpg)
For this final project, the case used to create the model table is transaction history data. This model table will detail each transaction recorded in the ANZ bank dataset. At this stage, the model will be split into one fact table and two dimension tables:
- **fact table**: records each transaction and supporting information recorded in the dataset
- **dim_customer**: records each customer who uses ANZ bank to conduct banking transactions
- **dim_merchant**: records each business person/merchant who transacts with an ANZ bank customer

### 5.3 Mart Table
![data flow scenario](.\documentation\table_mart.png)
Based on the Model Table, we can separate into 5 mart tables for the solution of this case:
- **Mart_payment_by_age**: stores customer payment transaction data based on age per month.
- **Mart_total_transaction_by_desc**: stores customer payment transaction data based on payment type per day.
- **Mart_transaction_by_location**: stores customer and merchant location data.
- **Mart_credit_by_date**: stores customer cash receipts per day.
- **Mart_monthly_transaction**: stores the total number of transactions per month.

## 6. Directed Acyclic Graph (DAG) Structure
A directed acyclic graph (DAG) is a graph in Airflow that represents the flow of tasks/programs to be executed in Airflow. In this section, will explain the program's execution flow structure. There are 2 DAGs for this final project:
### 6.1 Generate Dummy Data
![data flow scenario](.\documentation\generate_dummy_data.png)
In general, the DAG Generate Dummy Data will:
- data_source_generate: Create 500-1,000 rows of dummy data that will be generated into a .csv file
- data_source_ingestion: Insert the .csv file into Google Cloud Storage
- delete_files: Delete the .csv file on the local machine

### 6.2 ETL process
![data flow scenario](.\documentation\etl_process.png)
In general, this DAG ETL process will:
- Create_table: Checks whether the schema and table you want to use already exist in BigQuery. If the schema and table, or one or both, don't exist, the missing schema and table will be created first. If they already exist, this step will be skipped.
- Insert_raw_data: Inserts data from a .csv file in Google Cloud Storage into the raw data table in BigQuery.
- Transform_process: Executes the Data Build Tool (DBT) program, which will transform and create the Staging table, model, and mart.

## 7. Dashboard Visualization Result
![data flow scenario](.\documentation\dashboard.jpg)
In general, this visualization illustrates customer behavior when using transaction services (both payments and deposits) at ANZ Bank. The summarized information includes:
- Proportion of payment types used by customers
- Profile of ANZ Bank customer population by location of residence
- Distribution of ANZ Bank customer transaction activity by age
- Customer transaction activity by time

## 8 Things that can be approved
- Lack of coding flexibility: -> In the future, the program could be made more flexible and reusable for various use cases.
- Building an ETL program specifically for this case (data production per day spanning one day, not two days) -> In the future, an ETL program could be created that can handle a wider range of data.
- There is still the possibility of errors in certain cases -> Error handling implementation needs to be improved.
