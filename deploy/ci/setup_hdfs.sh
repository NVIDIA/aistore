#!/bin/bash

set -x

# Based on: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html

# Setup ssh server and keys.
/etc/init.d/ssh start
mkdir ~/.ssh
ssh-keygen -t rsa -f ~/.ssh/id_rsa -q -P ""
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

pushd /hadoop-3.3.0 || exit 1

cat <<EOF >> etc/hadoop/hadoop-env.sh
export HDFS_NAMENODE_USER="root"
export HDFS_DATANODE_USER="root"
export HDFS_SECONDARYNAMENODE_USER="root"
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
EOF

cat > etc/hadoop/core-site.xml <<EOF
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOF

cat > etc/hadoop/hdfs-site.xml <<EOF
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
EOF

bin/hdfs namenode -format
sbin/start-dfs.sh

popd || exit 1
