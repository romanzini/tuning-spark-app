"""
Spark App: yelp-dataset-data-gen-skew
Author: Rodrigo Romanzini

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/yelp-dataset-data-gen-skew.py
"""

from utils.utils import init_spark_session, list_files
from pyspark.sql.functions import current_date


def main():

    spark = init_spark_session("yelp-dataset-data-gen-skew")

    file_loc_reviews = "s3a://warehouse/storage/yelp/parquet/review/*.parquet"
    #list_files(spark, file_loc_reviews)
    df_reviews = spark.read.parquet(file_loc_reviews)

    # TODO create skewed data using today's [date].
    today = current_date()
    skew_multiplier = 5

    df_reviews_skew = df_reviews.withColumn("date", today)

    df_reviews_current_date = df_reviews_skew
    for _ in range(skew_multiplier - 1):
        df_reviews_current_date = df_reviews_current_date.union(df_reviews_skew)

    df_reviews_last = df_reviews.union(df_reviews_current_date)

    # TODO write in delta format.
    output_path = "s3a://warehouse/storage/delta/yelp/skew/dataset/"
    df_reviews_last.write.mode("overwrite").parquet(output_path)
    df_reviews_last.count()


if __name__ == "__main__":
    main()
