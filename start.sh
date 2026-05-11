#!/bin/bash
set -e
GREEN='\033[0;32m'; NC='\033[0m'
ok() { echo -e "${GREEN}[✓]${NC} $1"; }

HADOOP_HOME=${HADOOP_HOME:-/home/hadoop/hadoop-3.3.1}
SPARK_HOME=${SPARK_HOME:-/home/hadoop/spark-3.4.1-bin-hadoop3}

ok "Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh

ok "Starting YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

ok "Starting Spark..."
$SPARK_HOME/sbin/start-all.sh

echo ""
ok "All services started"
echo "  HDFS NameNode : http://192.168.56.10:9870"
echo "  YARN ResourceManager : http://192.168.56.10:8088"
echo "  Spark Master  : http://192.168.56.10:8080"
