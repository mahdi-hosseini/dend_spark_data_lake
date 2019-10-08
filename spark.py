import configparser
import os

from pyspark.sql import SparkSession


class Singleton(type):
    def __init__(cls, name, bases, attrs, **kwargs):
        super().__init__(name, bases, attrs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class SparkSession(metaclass=Singleton):
    def __init__(
        self, config_file: str, aws_env_vars: list, extra_jars: list
    ) -> None:
        self.configure_environment(config_file, aws_env_vars)
        self.spark_context: SparkSession = self.create_session(extra_jars)

    @classmethod
    def get_instance(cls):
        """
        Always returns same instance of class
        """
        return SparkSession()

    def create_session(self, extra_jars: list) -> SparkSession:
        """
        Instantiate a SparkSession with additional jars

        :param extra_jars: list of Maven coordinates of jars to include on
                            the driver and executor classpaths

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

        :param config_file: Path to ConfigParser INI file
        :param aws_env_vars: Options to pick from 'aws' section in the INI file

        """
        try:
            config = configparser.ConfigParser()
            config.read(config_file)
        except configparser.Error:
            raise ValueError("Could not parse configuration file")
        else:
            for var in aws_env_vars:
                try:
                    os.environ[var] = config.get("aws", var)
                except (
                    configparser.NoOptionError,
                    configparser.NoSectionError,
                ):
                    raise KeyError(
                        "Could not find configuration option '{var}'"
                        " in section 'aws'."
                    )
