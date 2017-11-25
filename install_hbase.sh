#!/bin/bash

cd ~
mkdir tools
cd tools

sudo apt install -y openjdk-8-jre

wget -N -c  http://apache.claz.org/hbase/1.3.1/hbase-1.3.1-bin.tar.gz
tar xvzf hbase-1.3.1-bin.tar.gz

cat << EOF >> ~/.profile
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HBASE_HOME=/home/ubuntu/tools/hbase-1.3.1
export PATH=\$PATH:\$HBASE_HOME/bin
start-hbase.sh
EOF

rm hbase-1.3.1-bin.tar.gz
