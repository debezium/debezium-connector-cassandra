#!/bin/sh

DSE_VERSION=6.8.16
2>/dev/null 1>&2 mvn dependency:get -Dartifact=com.datastax:dse-db-all:${DSE_VERSION} -o && mvn dependency:get -Dartifact=com.datastax:dse-commons:${DSE_VERSION} -o && mvn dependency:get -Dartifact=io.netty:netty-all:4.1.25.7.dse
EXISTS=$?
if [ ${EXISTS} -eq 0 ]; then
	echo "DSE artifacts already installed"
	exit 0
fi

DOCKER_DSE_SERVER_DIR=.docker-dse-server
DSE_SERVER_DIR=${DOCKER_DSE_SERVER_DIR}/${DSE_VERSION}
mkdir -p ${DSE_SERVER_DIR}

docker pull datastax/dse-server:${DSE_VERSION}
CONT_ID=$(docker create datastax/dse-server:${DSE_VERSION})
docker export ${CONT_ID} -o "${DSE_SERVER_DIR}/dse-server.tar.gz"
docker rm ${CONT_ID}

CASSANDRA_LIB_DIR=opt/dse/resources/cassandra/lib
tar -xf ${DSE_SERVER_DIR}/dse-server.tar.gz -C ${DSE_SERVER_DIR} ${CASSANDRA_LIB_DIR}/dse-db-all-${DSE_VERSION}.jar ${CASSANDRA_LIB_DIR}/dse-commons-${DSE_VERSION}.jar ${CASSANDRA_LIB_DIR}/netty-all-4.1.25.7.dse.jar

mvn install:install-file -DgroupId=com.datastax -DartifactId=dse-db-all -Dversion=${DSE_VERSION} -Dpackaging=jar -Dfile=${DSE_SERVER_DIR}/${CASSANDRA_LIB_DIR}/dse-db-all-${DSE_VERSION}.jar
mvn install:install-file -DgroupId=com.datastax -DartifactId=dse-commons -Dversion=${DSE_VERSION} -Dpackaging=jar -Dfile=${DSE_SERVER_DIR}/${CASSANDRA_LIB_DIR}/dse-commons-${DSE_VERSION}.jar
mvn install:install-file -DgroupId=io.netty -DartifactId=netty-all -Dversion=4.1.25.7.dse -Dpackaging=jar -Dfile=${DSE_SERVER_DIR}/${CASSANDRA_LIB_DIR}/netty-all-4.1.25.7.dse.jar

rm -rf ${DOCKER_DSE_SERVER_DIR}
