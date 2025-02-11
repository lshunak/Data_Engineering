#!/bin/bash

# Function to check if a process is running
is_process_running() {
    jps | grep "$1" > /dev/null
    return $?
}

# Function to start Hadoop services
start_hadoop() {
    if ! is_process_running "NameNode"; then
        echo "Starting Hadoop services..."
        $HADOOP_HOME/sbin/start-dfs.sh
        sleep 10
        
        # Create required directories
        hdfs dfs -mkdir -p /user/lshunak/wikipedia
        hdfs dfs -chmod 755 /user/lshunak/wikipedia
    else
        echo "Hadoop services are already running"
    fi
}

# Function to stop Hadoop services
stop_hadoop() {
    if is_process_running "NameNode"; then
        echo "Stopping Hadoop services..."
        $HADOOP_HOME/sbin/stop-dfs.sh
    else
        echo "Hadoop services are not running"
    fi
}

# Main logic
case "$1" in
    start)
        start_hadoop
        ;;
    stop)
        stop_hadoop
        ;;
    restart)
        stop_hadoop
        sleep 5
        start_hadoop
        ;;
    status)
        if is_process_running "NameNode"; then
            echo "Hadoop services are running"
            jps
        else
            echo "Hadoop services are not running"
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
esac

exit 0