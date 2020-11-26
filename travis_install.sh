#!/bin/bash -eux
sudo apt-get update
sudo apt-get -q install -y wget openjdk-8-jdk-headless
#sudo update-java-alternatives -s java-8-oracle

# download and install hbase 2.2.x
ver='2.2.5'
tarball="hbase-${ver}-bin.tar.gz"
wget -q -O /tmp/${tarball} https://www-us.apache.org/dist/hbase/${ver}/${tarball}

tar -xzf /tmp/${tarball} -C /var/tmp/

#configuration
cd /var/tmp/hbase-${ver}

cat <<EOF >conf/hbase-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>file:///var/tmp/hbase</value> 
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/var/tmp/zookeeper</value>
  </property>
  <property>
    <name>hbase.unsafe.stream.capability.enforce</name>
    <value>false</value>
  </property>
</configuration>
EOF

export HBASE_HOME=/var/tmp/hbase-${ver}
export PATH=$PATH:$HBASE_HOME/bin

#start hbase server and thriftserver
bin/start-hbase.sh
sleep 2
bin/hbase-daemon.sh start thrift2
sleep 2