"""
Spark App: yelp-dataset-shuffle-owshq-3
Author: Luan Moreno

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/yelp-dataset-shuffle-owshq-3.py
"""

from pyspark.sql.functions import col, broadcast
from sparkmeasure import StageMetrics
from utils.utils import init_spark_session, list_files


def main():

    # TODO .config("spark.sql.shuffle.partitions", "50")
    # TODO .config("spark.default.parallelism", "50")
    # TODO .config("spark.sql.files.maxPartitionBytes", "67108864") # 32, 64, 128, 256 MB
    spark = init_spark_session("yelp-dataset-shuffle-owshq-3")

    stage_metrics = StageMetrics(spark)
    stage_metrics.begin()

    file_loc_users = "./storage/yelp/users/*.parquet"
    list_files(spark, file_loc_users)
    df_users = spark.read.parquet(file_loc_users)

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

    # TODO add new join with users dataset [new business requirement]
    num_partitions = 50

    df_join_reviews_business_repart = df_join_reviews_business.repartition(num_partitions, "user_id")
    user_df_repart = df_users.repartition(num_partitions, "user_id")

    df_join_users_repartition = df_join_reviews_business_repart.alias("rb").join(user_df_repart.alias("u"), col("rb.user_id") == col("u.user_id"))
    df_join_users_repartition.show()
    df_join_users_repartition.explain()

    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

    metrics = "./metrics/yelp-dataset-shuffle-owshq-3/"
    df_stage_metrics = stage_metrics.create_stagemetrics_DF("PerfStageMetrics")
    df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

    df_aggregated_metrics = stage_metrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    df_aggregated_metrics.write.mode("overwrite").json(metrics + "stagemetrics_agg")


if __name__ == "__main__":
    main()
