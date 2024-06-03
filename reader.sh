#!/bin/bash
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

while true
do
    keys=`redis-cli -h ${CLUSTER_NAME}-m -p 6379 keys '*'`
    if [ "$keys" ]; then
        echo "$keys" | while IFS= read -r key; do
            type=`echo | redis-cli type "$key"`
            if [[ $key == _spark* ]]; then
                continue
            fi
            case "$type" in
                string) value=`echo | redis-cli -h ${CLUSTER_NAME}-m -p 6379 get "$key"`;;
                hash) value=`echo | redis-cli -h ${CLUSTER_NAME}-m -p 6379 hgetall "$key"`;;
                set) value=`echo | redis-cli -h ${CLUSTER_NAME}-m -p 6379 smembers "$key"`;;
                list) value=`echo | redis-cli -h ${CLUSTER_NAME}-m -p 6379 lrange "$key" 0 -1`;;
                zset) value=`echo | redis-cli -h ${CLUSTER_NAME}-m -p 6379 zrange "$key" 0 -1 withscores`;;
            esac
            echo "> $key ($type):"
            echo "$value" | sed -E 's/^/    /'
        done
    fi
    sleep 1
    echo "--------------------------------"
done