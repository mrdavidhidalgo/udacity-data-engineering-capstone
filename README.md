# Udacity's Capstone Project

This project builds a pipeline using Apache Airflow based on public datasets with Amazon redshift as main database

## Datasets
The datasets were taken from www.kaggle.com

- [US accidents](https://smoosavi.org/datasets/us_accidents)  
- [World cities](https://www.kaggle.com/okfn/world-cities)

One of the requirements of this project was to handle at least two sources and more than 1 million rows,
 dataset [US accidents](https://smoosavi.org/datasets/us_accidents) has more than 3 million records

## Main technologies used:

### Apache Airflow to build the pipeline
Apache Airflow is a great tool to create and maintain data-pipelines, It is easy to use and support

### Jupyter Notebook to explore datasets
Jupiter Notebook is a good tool to execute python commands step-by-step, It is useful to interact with data using python

### Amazon S3 to load the pre-processed data
It is the backbone of AWS, it is a virtually unlimited storage service, ease to scale and good integration with Amazon Redshift

### Amazon Redshift as data-warehouse
It is a database that supports terabytes of data, with horizontal scaling and excellent for bigdata

## The steps of the process are:

1. Data exploration and pre-processing of  datasets with Jupyter notebook, looking for missing data or particular cases
2. Transformed datasets are uploaded to S3 using python
3. A Redshift cluster is create by code
4. A pipeline is built on Apache Airflow to load data from Amazon S3 to Redshift into staging tables
5. Some queries are executed in redshift

## How often the data should be updated?
The data must be updated every month, this because the accident records are totaled every month, another viable option would be annually, it should be remembered that these data must be consolidated before being processed, so the periodicity cannot be less

## Particular Scenarios

### Data was increased by 100x

If the data increased by 100x, we must consider some issues: Storage or partitioning of data

#### Storage
The storage is always a difficult topic, but Amazon S3 still being a good option and Amazon Redshift scale horizontally, another good option is Apache Spark or Snowflake

#### The partitioning of data
Using large files is not a good idea, S3 supports files divided into many parts and is easy to use with Redshift, another option is to partition by data, such as record date.

## Pipelines were run on a daily basis by 7am
In this moment the pipeline take around 5 minutes to load, in the worst case it is always possible to partition the data to process in parallel, this thanks to the capabilities of Redshift.

## The database needed to be accessed by 100+ people.

The main problem of increasing users in the database is the way Redshift operates, the easiest way to meet this need is by generating clusters with duplicate data to make queries.
