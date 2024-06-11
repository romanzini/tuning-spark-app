"""
Spark App: yelp-dataset-skew-owshq-salting
Author: Rodrigo Romanzini

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/yelp-dataset-skew-owshq-salting.py
"""

from sparkmeasure import StageMetrics
from utils.utils import init_spark_session, list_files
from pyspark.sql.functions import col, lit, rand, concat


def main():

    spark = init_spark_session("yelp-dataset-skew-owshq-salting")

    stage_metrics = StageMetrics(spark)
    stage_metrics.begin()

    file_loc_reviews = "./storage/delta/yelp/skew/dataset/*.parquet"
    list_files(spark, file_loc_reviews)
    df_reviews = spark.read.parquet(file_loc_reviews)
    print(f"total rows: {df_reviews.count()}")

    file_loc_business = "./storage/yelp/business/*.parquet"
    list_files(spark, file_loc_business)
    df_business = spark.read.parquet(file_loc_business)

    # TODO salting technique [400 sweet spot]
    salt_factor = 400

    df_reviews_salt = df_reviews.withColumn(
        "salt_date",
        concat(col("date"), lit("_"), (rand() * salt_factor).cast("int"))
    ).withColumn(
        "salt_business_id",
        concat(col("business_id"), lit("_"), (rand() * salt_factor).cast("int"))
    )
    # TODO print(df_reviews_salt.limit(10).show())

    df_business_salt = df_business.withColumn(
        "salt_business_id",
        concat(col("business_id"), lit("_"), (rand() * salt_factor).cast("int"))
    )

    # TODO salting dataset
    df_reviews_salt.createOrReplaceTempView("reviews")
    df_business_salt.createOrReplaceTempView("business")

    # TODO use salting
    df_join_skew = spark.sql("""
        SELECT r.review_id, r.user_id, r.date, b.name, b.city
        FROM reviews r
        JOIN business b
        ON r.salt_business_id = b.salt_business_id
        WHERE r.salt_date LIKE '2024-06-06%'
    """).coalesce(200)  # TODO decrease partitions [before writing]
    print(f"total rows skewed query: {df_join_skew.count()}")

    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

    metrics = "./metrics/yelp-dataset-skew-owshq-salting/"
    df_stage_metrics = stage_metrics.create_stagemetrics_DF("PerfStageMetrics")
    df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

    df_stage_metrics_summary = stage_metrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    df_stage_metrics_summary.write.mode("overwrite").json(metrics + "stagemetrics_summary")


if __name__ == "__main__":
    main()
