CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

kafka-console-consumer.sh --group my-consumer-group --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --topic prj-2-anomalies --from-beginning