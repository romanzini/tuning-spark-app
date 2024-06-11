"""
Spark App: yelp-dataset-data-gen-skew
Author: Rodrigo Romanzini

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/yelp-data-json-to-parquet.py
"""

from utils.utils import init_spark_session


def main():

    spark = init_spark_session("yelp-data-json-to-parquet")

    list_path = ['business','checkin','review','tip','user']

    input_path_json = "s3a://warehouse/storage/yelp/json/"
    extension_json = "/*.json"

    output_path_patquet = "s3a://warehouse/storage/yelp/parquet/"
    
    for i in list_path:
        # read json file
        file_json = input_path_json + i + extension_json
        df_json = spark.read.json(file_json)
        print(f"path json: {file_json}")
        print(f"number of rows: {df_json.count()}")

        # write in parquet format
        file_parquet = output_path_patquet + i
        print(f"path parquet: {file_parquet}")
        df_json.write.mode("overwrite").parquet(file_parquet)

if __name__ == "__main__":
    main()
