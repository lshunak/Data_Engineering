#!/bin/bash
set -e

echo "Starting Hadoop services..."

# Ensure JAVA_HOME is set
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Format namenode if not already formatted
if [ ! -f "/hadoop/dfs/name/current/VERSION" ]; then
    echo "Formatting namenode..."
    hdfs namenode -format -force
fi

# Start HDFS
echo "Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh

# Wait for namenode to be up
echo "Waiting for namenode to be up..."
timeout 30 bash -c 'until hdfs dfs -ls / &>/dev/null; do sleep 1; done' || {
    echo "Namenode failed to start within timeout"
    exit 1
}

# Start YARN
echo "Starting YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

echo "Creating initial directories..."
hdfs dfs -mkdir -p /tmp /user
hdfs dfs -chmod 777 /tmp
hdfs dfs -chmod 777 /user

echo "All Hadoop services started successfully"

# Keep container running and monitor services
while true; do
    sleep 5
    if ! jps | grep -q NameNode; then
        echo "NameNode is not running! Exiting..."
        exit 1
    fi
    echo "Services are running..."
done