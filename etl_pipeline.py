from datetime import datetime

from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    desc,
    hour,
    lit,
    month,
    row_number,
    udf,
    weekofyear,
    when,
    year,
)
from pyspark.sql.types import LongType, TimestampType
from spark import SparkSession

def urljoin(*args):
    return "/".join(args)

class ETLPipeline:
    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session: SparkSession = spark_session

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

        # de-dupe based on artist_id and song_id
        return (
            songs_table.withColumn(
                "row_number",
                row_number().over(
                    Window.partitionBy("song_id").orderBy(
                        "artist_id", "song_id"
                    )
                ),
            )
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

        # de-dupe based on artist_id and artist_name
        return (
            artists_table.withColumn(
                "row_number",
                row_number().over(
                    Window.partitionBy("artist_id").orderBy(
                        "artist_id", "name"
                    )
                ),
            )
            .filter(col("row_number") == 1)
            .drop("row_number")
        )

    def process_song_data(self, input_data, output_data) -> None:
        """
        Perform ETL on song dataset
        TODO: ADD DOCSTRING
        """
        song_data_df = self.spark_session.read.json(input_data)

        songs_table_df = self.get_songs_table(song_data_df)
        songs_table_df.write.partitionBy("year", "artist_id").parquet(
            urljoin(output_data, "songs")
        )

        artists_table_df = self.get_artists_table(song_data_df)
        artists_table_df.write.parquet(urljoin(output_data, "artists"))

    def get_users_table(self, df: DataFrame) -> DataFrame:
        """
        1. Rename columns to snake case
        2. Get first occurrence of each user ordered by timestamp in descending
           order to de-duplicate data
        """
        # extract columns for users table
        users_table = df.select(
            col("userId").alias("user_id"),
            col("firstname").alias("first_name"),
            col("lastname").alias("last_name"),
            col("gender"),
            col("level"),
            col("ts"),
        )

        return (
            users_table.withColumn(
                "row_number",
                row_number().over(
                    Window.partitionBy("user_id").orderBy(
                        "user_id", desc("ts")
                    )
                ),
            )
            .filter(col("row_number") == 1)
            .drop("row_number", "ts")
        )

    def get_time_table(self, df: DataFrame) -> DataFrame:
        """
        TODO: ADD DOCSTRING
        """
        # create timestamp column from original timestamp column
        get_timestamp = udf(lambda x: str(datetime.fromtimestamp(x / 1000.0)))
        df = df.withColumn(
            "timestamp",
            date_format(
                get_timestamp(col("ts")), "yyyy-MM-dd HH:mm:ss.SSSXXX"
            ),
        )

        # create datetime column from original timestamp column
        df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

        # extract columns to create time table
        time_table = df.select(
            col("timestamp").alias("start_time"),
            hour("timestamp").alias("hour"),
            dayofmonth("timestamp").alias("day"),
            weekofyear("timestamp").alias("week"),
            month("timestamp").alias("month"),
            year("timestamp").alias("year"),
            date_format(col("timestamp"), "u").alias("weekday"),
        )

        return time_table.dropDuplicates()

    def clean_log_data(self, df: DataFrame) -> DataFrame:
        """
        1. Remove rows with nulls and empty strings and cast
        2. Cast userId to LongType
        3. Filter by actions for songplays
        """
        df = df.dropna(
            how="any",
            subset=[
                "artist",
                "firstName",
                "gender",
                "lastName",
                "length",
                "level",
                "page",
                "sessionId",
                "song",
                "ts",
                "userAgent",
                "userId",
            ],
        )

        # remove empty strings
        df = df.filter(
            (col("artist") != "")
            | (col("firstName") != "")
            | (col("gender") != "")
            | (col("lastName") != "")
            | (col("level") != "")
            | (col("song") != "")
            | (col("userAgent") != "")
            | (col("userId") != "")
        )

        # filter by actions for song plays
        return df.withColumn("userId", col("userId").cast(LongType())).filter(
            df.page == "NextSong"
        )

    def process_log_data(self, input_data, output_data) -> None:
        """
        Perform ETL on user activity log dataset
        TODO: ADD DOCSTRING
        """
        log_data_df = self.spark_session.read.json(input_data)
        log_data_df = self.clean_log_data(log_data_df)

        users_table_df = self.get_users_table(log_data_df)
        users_table_df.write.parquet(urljoin(output_data, "users"))

        time_table_df = self.get_time_table(log_data_df)
        time_table_df.write.partitionBy("year", "month").parquet(
            urljoin(output_data, "time")
        )

        # read in song data to use for songplays table
        songs_table_df = self.spark_session.read.parquet(
            urljoin(output_data, "songs")
        )
        artists_table_df = self.spark_session.read.parquet(
            urljoin(output_data, "artists")
        )

        songs_table_df = songs_table_df.join(
            artists_table_df,
            songs_table_df.artist_id == artists_table_df.artist_id,
        ).select(
            songs_table_df["song_id"],
            songs_table_df["title"],
            songs_table_df["duration"],
            songs_table_df["artist_id"],
            artists_table_df["name"],
        )

        # extract columns from joined song and log datasets to create songplays table
        songplays_table = songs_table_df.join(
            log_data_df,
            (log_data_df.artist == songs_table_df.name)
            & (log_data_df.song == songs_table_df.title)
            & (log_data_df.length == songs_table_df.duration),
        ).select(
            log_data_df["timestamp"].alias("start_time"),
            log_data_df["userId"].alias("user_id"),
            log_data_df["level"],
            songs_table_df["artist_id"],
            log_data_df["sessionId"].alias("session_id"),
            log_data_df["location"],
            log_data_df["userAgent"].alias("user_agent"),
            year("timestamp").alias("year"),
            month("timestamp").alias("month"),
        )

        # write songplays table to parquet files partitioned by year and month
        songplays_table.write.partitionBy("year", "month").parquet(
            urljoin(output_data, "songplays")
        )


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
