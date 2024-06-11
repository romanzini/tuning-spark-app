# Apache Spark & Trip Record Data Analysis [Spark Measure]

![Roadmap](./docs/perf-spark.png)
![Roadmap](./docs/perf-tuning-d1-game-change.png)
![Roadmap](./docs/perf-tuning-d2-3s.png)
![Rodmap](./docs/perf-tuning-d3.png)

## install homebrew (optional)
```shell
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

(echo; echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"') >> /home/romanzini/.bashrc
eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"

sudo apt-get install build-essential

brew install gcc
```

## verify python
```shell
python3 --version
```

## install & create env
```shell
pip install virtualenv

virtualenv venv
```

## activate
```shell
source venv/bin/activate
```

## deactivate
```shell
source venv/bin/deactivate
```

## install java
```shell
sudo apt install openjdk-11-jre-headless
```

## verify local spark installation
```shell
pyspark --version

spark-submit --help
http://localhost:4040/jobs/
```

## install required packages
```shell
pip install -r requirements.txt
```

## download jars files and mv to config/spark/jars
```shell
curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar 
curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar 
curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar 
curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar 
mv s3-2.18.41.jar config/spark/jars 
mv aws-java-sdk-1.12.367.jar config/spark/jars 
mv delta-core_2.12-2.2.0.jar config/spark/jars 
mv delta-storage-2.2.0.jar config/spark/jars
```

## create .env file for variables
```shell
.env

APP_SRC_PATH=/home/romanzini/projetos/tuning-spark-app/src
APP_STORAGE_PATH=/home/romanzini/projetos/tuning-spark-app/storage
APP_LOG_PATH=/home/romanzini/projetos/tuning-spark-app/logs
APP_METRICS_PATH=/home/romanzini/projetos/tuning-spark-app/metrics
APP_LOGSTASH_PATH=/home/romanzini/projetos/tuning-spark-app/events/
```

## build spark docker images [spark & history server]
```shell
docker build -t owshq-spark:3.5 -f Dockerfile.spark . 
docker build -t owshq-spark-history-server:3.5 -f Dockerfile.history .
```

## run spark cluster & history server on docker
```shell
docker-compose up -d
docker ps

docker logs spark-master
docker logs spark-worker-1
docker logs spark-worker-2
docker logs spark-history-server
```

## download files & save on minio [storage/fhvhv/2022] 
```shell
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
```

## download files & save on minio [storage/yelp] 
```shell
https://www.yelp.com/dataset/download
```

## execute spark application
```shell
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-py-strawberry.py
```

## access spark history server
```shell
http://localhost:8080/
http://localhost:18080/
```

## access MinIO UI
```shell
http://localhost:9001/
```

## tier down resources
```shell
docker-compose down
```
