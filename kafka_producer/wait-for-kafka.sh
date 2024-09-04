#!/bin/bash
set -e

host="$1"
port="$2"
shift 2
cmd="$@"

until timeout 1 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null
do
  echo "Waiting for Kafka to be ready... (host: $host, port: $port)"
  sleep 1
done

echo "Kafka is up - executing command"
exec $cmd