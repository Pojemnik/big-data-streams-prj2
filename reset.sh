#(Re)Start docker container
docker stop redis
docker rm redis
docker run --name redis -p 6379:6379 -d redis redis-server --save 60 1 --loglevel warning

#Setup kafka topics
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --topic prj-2-input
kafka-topics.sh --create --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --replication-factor 1 --partitions 1 --topic prj-2-input

kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --topic prj-2-anomalies
kafka-topics.sh --create --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --replication-factor 1 --partitions 1 --topic prj-2-anomalies

#Remove spark checkpoints
hadoop fs -rm -r tmp

#Wait for the container to start
sleep 10