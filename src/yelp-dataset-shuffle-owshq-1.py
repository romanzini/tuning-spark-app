"""
Spark App: yelp-dataset-shuffle-owshq-1
Author: Rodrigo Romanzini

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/yelp-dataset-shuffle-owshq-1.py
"""

from pyspark.sql.functions import col, broadcast
from sparkmeasure import StageMetrics
from utils.utils import init_spark_session, list_files


def main():

    spark = init_spark_session("yelp-dataset-shuffle-owshq-1")

    stage_metrics = StageMetrics(spark)
    stage_metrics.begin()

    file_loc_business = "./storage/yelp/business/*.parquet"
    list_files(spark, file_loc_business)
    df_business = spark.read.parquet(file_loc_business)

    file_loc_reviews = "./storage/yelp/reviews/*.parquet"
    list_files(spark, file_loc_reviews)
    df_reviews = spark.read.parquet(file_loc_reviews)

    # TODO broadcasting a small dataset: df_business
    df_join_reviews_business = df_reviews.alias("r").join(broadcast(df_business.alias("b")), col("r.business_id") == col("b.business_id"))
    df_join_reviews_business.show()
    df_join_reviews_business.explain()

    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

    metrics = "./metrics/yelp-dataset-shuffle-owshq-1/"
    df_stage_metrics = stage_metrics.create_stagemetrics_DF("PerfStageMetrics")
    df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

    df_aggregated_metrics = stage_metrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    df_aggregated_metrics.write.mode("overwrite").json(metrics + "stagemetrics_agg")


if __name__ == "__main__":
    main()
