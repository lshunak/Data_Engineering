FROM apache/hadoop:3.4

# Switch to root user
USER root

RUN which java | xargs readlink -f | sed 's|/bin/java||' > /tmp/java_home && \
    echo "export JAVA_HOME=$(cat /tmp/java_home)" >> /etc/profile && \
    echo "export JAVA_HOME=$(cat /tmp/java_home)" >> /etc/environment && \
    echo "export JAVA_HOME=$(cat /tmp/java_home)" >> ~/.bashrc

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Create necessary directories
RUN mkdir -p /hadoop/dfs/name && \
    mkdir -p /hadoop/dfs/data && \
    mkdir -p $HADOOP_HOME/logs && \
    chmod -R 777 /hadoop && \
    chmod -R 777 $HADOOP_HOME/logs

# Configure core-site.xml
RUN echo '<?xml version="1.0" encoding="UTF-8"?>\
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\
    <configuration>\
    <property>\
    <name>fs.defaultFS</name>\
    <value>hdfs://localhost:9000</value>\
    </property>\
    </configuration>' > $HADOOP_HOME/etc/hadoop/core-site.xml

# Configure hdfs-site.xml
RUN echo '<?xml version="1.0" encoding="UTF-8"?>\
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\
    <configuration>\
    <property>\
    <name>dfs.replication</name>\
    <value>1</value>\
    </property>\
    <property>\
    <name>dfs.namenode.name.dir</name>\
    <value>/hadoop/dfs/name</value>\
    </property>\
    <property>\
    <name>dfs.datanode.data.dir</name>\
    <value>/hadoop/dfs/data</value>\
    </property>\
    </configuration>' > $HADOOP_HOME/etc/hadoop/hdfs-site.xml

# Copy startup script
COPY start-services.sh /start-services.sh
RUN chmod +x /start-services.sh

# Expose ports
EXPOSE 9870 9864 8088 9000

ENTRYPOINT ["/start-services.sh"]