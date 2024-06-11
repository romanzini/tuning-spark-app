# TODO coalesce or repartition the dataset to avoid skew

"""
Spark App: yelp-dataset-skew
Author: Rodrigo Romanzini

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/yelp-dataset-skew.py
"""

from sparkmeasure import StageMetrics
from utils.utils import init_spark_session, list_files


def main():

    spark = init_spark_session("yelp-dataset-skew")

    stage_metrics = StageMetrics(spark)
    stage_metrics.begin()

    file_loc_reviews = "./storage/delta/yelp/skew/dataset/*.parquet"
    list_files(spark, file_loc_reviews)
    df_reviews = spark.read.parquet(file_loc_reviews)
    print(f"total rows: {df_reviews.count()}")

    file_loc_business = "./storage/yelp/business/*.parquet"
    list_files(spark, file_loc_business)
    df_business = spark.read.parquet(file_loc_business)

    df_reviews.createOrReplaceTempView("reviews")
    df_business.createOrReplaceTempView("business")

    # TODO date of 2024-06-06 = 409.655.595 {skewed}
    df_skew_yelp = spark.sql("""
        SELECT r.date,
            COUNT(*) AS qtd
        FROM reviews r
        WHERE r.date = '2024-06-06'
        GROUP BY r.date
        ORDER BY qtd DESC
    """)

    # TODO query using skewed date [2024-06-06] = 1.222.244.440
    df_join_skew = spark.sql("""
        SELECT r.review_id, r.user_id, r.date, b.name, b.city
        FROM reviews r
        JOIN business b
        ON r.business_id = b.business_id
        WHERE r.date = '2024-06-06'
    """)
    print(f"total rows skewed query: {df_join_skew.count()}")

    # TODO: stage metrics
    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

    metrics = "./metrics/yelp-dataset-skew/"
    df_stage_metrics = stage_metrics.create_stagemetrics_DF("PerfStageMetrics")
    df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

    df_stage_metrics_summary = stage_metrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    df_stage_metrics_summary.write.mode("overwrite").json(metrics + "stagemetrics_summary")


if __name__ == "__main__":
    main()
