import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    hour,
    month,
    udf,
    weekofyear,
    year,
)


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
        self.session: SparkSession = self.create_session(extra_jars)

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

    def process_song_data(self, input_data, output_data) -> None:
        """
        Perform ETL on song dataset
        """
        pass

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

    input_data = "s3a://udacity-dend/"
    output_data = ""

    spark.process_song_data(input_data, output_data)
    spark.process_log_data(input_data, output_data)


if __name__ == "__main__":
    main()
