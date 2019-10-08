# dend_spark_data_lake

## 1. Summary and purpose

Sparkify wants to put the data into the hands of data analysts to be able to get a better understanding of user demographics, listening profile and potentially boost conversions and reduce churn. Currently the data is stored in raw log files that are not accessible for analysis.

The purpose of this workflow project is to perform dimensional modeling and store the transformed data into a data warehouse so that OLAP queries could be run against it. In order to facilitate that, we need to get the data out of its current normalized form that is suited for OLTP setting and store it in a S3 Data Lake in Parquet format.


## 2. Dataset Description

The data set for this project consists of music streaming activity log and and song data set. This data resides in a S3 bucket named `udacity-dend`.

### a. Activity log data

**Location**: `s3://udacity-dend/log-data`

Simulated Music streaming app activity logs (created using [event simulator](https://github.com/Interana/eventsim)) partitioned by year and month. The data set consists of 31 files with JSON-lines.

### b. Song data

**Location**: `s3://udacity-dend/song-data`

Subset of [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) partitioned by the first three letters of each song's track ID. The data set consists of 385,253 JSON files with individual songs.



## 3. Schema Design

The schema design uses star scheme and the act of listening is our fact (songplays table) which is additive and each one is constituted as a single user activity. Dimension tables are users, songs, artists, time. This would enable a data analyst/engineer/scientist to perform minimal number of joins to do their analysis and get results faster.


## 4. ETL pipeline

The Extract-Transfrom-Load (ETL) pipeline focuses on active users who have interacted with the streaming platform and listened to songs and so a filter is applied only on records that have `page = 'NextSong'`.

The pipeline is constructed to be run as a Spark job and data processing is done by using Spark DataFrame API. Both SparkSQL and DataFrame API undergo optimization through Catalyst optimizer so the choice of API is a matter of preference.

The pipeline steps are as follows:

### a. Extract + Transform

**Song data set**

* `songs` table: munge and de-duplicate song attributes
* `artists` table: de-duplicate artist attributes

**User activity log data set**

* `users` table: de-duplicate user attributes
* `time` table: created time attributes based on the Unix timestamp
* `songplays` table: join `artists` table with `songs` table to extract `artist_name` and do a final join on the first joined table to create all `songplays` attributes

### c. Load

**Song data set**

* `songs` table: partition by `year` and `artist_id` store as Parquet on S3
* `artists` table: store as Parquet on S3

**User activity log data set**

* `users` table: store as Parquet on S3
* `time` table: partition by `year` and `month` store as Parquet on S3
* `songplays` table: partition by `year` and `month` and store as Parquet on S3


## 5. Project Structure
#### README.md

Description of the project and explanation of the ETL pipeline

#### config/aws_keys.cfg

Python `ConfigParser` INI file hosting the AWS access keys

#### etl_pipeline.py

A Python module containing `ETLPipeline` class definition the performs all the steps described in section 4 above.

#### main.py

The entry point for the spark job

#### spark.py

A Python module for instantiation of a Spark session, this module is accessed by the pipeline.

## 6. How To Run

**Prerequisites:**

* Python 3.6
* AWS CLI
* PIP packages: PySpark, boto3
* Spark 2.4
* Spark JARs: `org.apache.hadoop:hadoop-aws:2.7.0`

**Steps:**

1. Create a Spark EMR cluster in Cluster mode

2. Consolidate all Python modules into `main.py`

3. Place your Python scripts in a S3 bucket that is accessible to EMR

For example:
* s3a://sparkify-spark-etl/code/main.py

4. Use `spark-submit` to create and run the job, the following command performs that:

```bash
aws emr add-steps \
    --cluster-id <emr_cluster_id> \
    --steps Type=spark,Name=SparkifyJob,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,s3a://sparkify-spark-etl/code/main.py],ActionOnFailure=CONTINUE
```
