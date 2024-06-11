"""
Spark App: elt-rides-fhvhv-spill
Author: Luan Moreno

spark.executor.memoryOverhead
- memory managed outside the java heap.
- network communication, especially during shuffles and joins, requires buffers to transfer data between nodes.
- might be additional memory required for managing the execution context, data serialization & deserialization, and other intermediate operations.

problem:
- memory pressure & fraction configuration
- join, aggregation, and sort operations
- data skew
- insufficient memory
- complex [nested transformations]

solution:
- partitions evenly distributed
- adjust overhead
- use broadcast joins
- optimize transformers [e.g. order by]

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-spill.py
"""

from sparkmeasure import StageMetrics
from utils.utils import init_spark_session, list_files


def main():

    # TODO .config("spark.sql.adaptive.enabled", "false")  # TODO true
    # TODO .config("spark.executor.memory", "2g")  # TODO 3g
    # TODO .config("spark.executor.memoryOverhead", "512m")  # TODO remove
    # TODO .config("spark.sql.shuffle.partitions", "200")  # TODO remove
    # TODO .config("spark.memory.fraction", "0.6")  # TODO remove
    spark = init_spark_session("elt-rides-fhvhv-spill")

    stage_metrics = StageMetrics(spark)
    stage_metrics.begin()

    file_fhvhv = "./storage/fhvhv/2022/*.parquet"
    list_files(spark, file_fhvhv)
    df_fhvhv = spark.read.parquet(file_fhvhv)

    file_zones = "./storage/zones.csv"
    list_files(spark, file_zones)
    df_zones = spark.read.option("delimiter", ",").option("header", True).csv(file_zones)

    df_fhvhv.createOrReplaceTempView("hvfhs")
    df_zones.createOrReplaceTempView("zones")

    df_rides = spark.sql("""
        SELECT hvfhs_license_num,
               zones_pu.Borough AS PU_Borough,
               zones_pu.Zone AS PU_Zone,
               zones_do.Borough AS DO_Borough,
               zones_do.Zone AS DO_Zone,
               request_datetime,
               pickup_datetime,
               dropoff_datetime,
               trip_miles,
               trip_time,
               base_passenger_fare,
               tolls,
               bcf,
               sales_tax,
               congestion_surcharge,
               tips,
               driver_pay,
               shared_request_flag,
               shared_match_flag
        FROM hvfhs
        INNER JOIN zones AS zones_pu
        ON CAST(hvfhs.PULocationID AS INT) = zones_pu.LocationID
        INNER JOIN zones AS zones_do
        ON hvfhs.DOLocationID = zones_do.LocationID
        ORDER BY request_datetime DESC
    """)

    df_rides.explain()
    df_rides.write.mode("overwrite").format("delta").save("./storage/delta/rides")

    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

    metrics = "./metrics/elt-rides-fhvhv-spill/"
    df_stage_metrics = stage_metrics.create_stagemetrics_DF("PerfStageMetrics")
    df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

    df_aggregated_metrics = stage_metrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    df_aggregated_metrics.write.mode("overwrite").json(metrics + "stagemetrics_agg")


if __name__ == "__main__":
    main()
