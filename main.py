from etl_pipeline import ETLPipeline, urljoin
from spark import SparkSession


def main():
    spark_session = SparkSession(
        config_file="config/aws_keys.cfg",
        aws_env_vars=["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
        extra_jars=["org.apache.hadoop:hadoop-aws:2.7.0"],
    )

    etl_pipeline = ETLPipeline(spark_session)

    s3_bucket_uri = "s3a://udacity-dend"
    output_data = "output"

    etl_pipeline.process_song_data(
        urljoin(s3_bucket_uri, "song_data/*/*/*/*.json"), output_data
    )
    etl_pipeline.process_log_data(
        urljoin(s3_bucket_uri, "log_data/*.json"), output_data
    )


if __name__ == "__main__":
    main()
