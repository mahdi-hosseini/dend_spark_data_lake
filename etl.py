import configparser
import os
from datetime import datetime
from urllib.parse import urljoin

from pyspark.sql import SparkSession, Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    desc,
    hour,
    lit,
    monotonically_increasing_id,
    month,
    row_number,
    udf,
    weekofyear,
    when,
    year,
)
from pyspark.sql.types import LongType, TimestampType


class Singleton(type):
    def __init__(cls, name, bases, attrs, **kwargs):
        super().__init__(name, bases, attrs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class Spark(metaclass=Singleton):
    def __init__(
        self, config_file: str, aws_env_vars: list, extra_jars: list
    ) -> None:
        self.configure_environment(config_file, aws_env_vars)
        self.spark_context: SparkSession = self.create_session(extra_jars)

    @classmethod
    def get_instance(cls):
        return Spark()

    def create_session(self, extra_jars: list) -> SparkSession:
        """
        Instantiate a SparkContext with hadoop-aws package
        """
        if not extra_jars:
            raise ValueError("No jar names were provided")

        return SparkSession.builder.config(
            "spark.jars.packages", ",".join(extra_jars)
        ).getOrCreate()

    def configure_environment(
        self, config_file: str, aws_env_vars: list
    ) -> None:
        """
        Set AWS keys as environment variables using a ConfigParser
        INI configuration file
        """
        try:
            config = configparser.ConfigParser()
            config.read(config_file)
        except configparser.Error:
            raise ValueError("Could not parse configuration file")
        else:
            try:
                for var in aws_env_vars:
                    os.environ[var] = config.get("aws", var)
            except (configparser.NoOptionError, configparser.NoSectionError):
                raise KeyError(
                    "Could not find configuration option '{var}'"
                    " in section 'aws'."
                )

    def get_songs_table(self, df: DataFrame) -> DataFrame:
        """
        TODO: ADD DOCSTRING
        """
        # extract columns to create songs table
        songs_table = df.select(
            col("song_id"),
            col("title"),
            col("artist_id"),
            col("year"),
            col("duration"),
        ).withColumn(
            "year", when(df["year"] == 0, lit(None)).otherwise(df["year"])
        )

        # dedupe based on artist_id and song_id
        window = Window.partitionBy("song_id").orderBy("artist_id", "song_id")

        return (
            songs_table.withColumn("row_number", row_number().over(window))
            .filter(col("row_number") == 1)
            .drop("row_number")
        )

    def get_artists_table(self, df: DataFrame) -> DataFrame:
        """
        TODO: ADD DOCSTRING
        """
        # extract columns to create artists table
        artists_table = df.select(
            col("artist_id"),
            col("artist_name").alias("name"),
            col("artist_location").alias("location"),
            col("artist_latitude").alias("latitude"),
            col("artist_longitude").alias("longitude"),
        )

        # dedupe based on artist_id and artist_name
        window = Window.partitionBy("artist_id").orderBy("artist_id", "name")

        return (
            artists_table.withColumn("row_number", row_number().over(window))
            .filter(col("row_number") == 1)
            .drop("row_number")
        )

    def process_song_data(self, input_data, output_data) -> None:
        """
        Perform ETL on song dataset
        TODO: ADD DOCSTRING
        """
        song_data_df = self.spark_context.read.json(input_data)

        songs_table_df = self.get_songs_table(song_data_df)
        songs_table_df.write.partitionBy("year", "artist_id").parquet(
            urljoin(output_data, "songs")
        )

        artists_table_df = self.get_artists_table(song_data_df)
        artists_table_df.write.parquet(urljoin(output_data, "artists"))

    def process_log_data(self, input_data, output_data) -> None:
        """
        Perform ETL on user activity log dataset
        """
        pass


def main():
    spark = Spark(
        config_file="dl.cfg",
        aws_env_vars=["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
        extra_jars=["org.apache.hadoop:hadoop-aws:2.7.0"],
    )

    s3_bucket_uri = "s3a://udacity-dend"
    output_data = "output"

    spark.process_song_data(
        urljoin(s3_bucket_uri, "song_data/*/*/*/*.json"), output_data
    )
    spark.process_log_data(
        urljoin(s3_bucket_uri, "log_data/*.json"), output_data
    )


if __name__ == "__main__":
    main()
