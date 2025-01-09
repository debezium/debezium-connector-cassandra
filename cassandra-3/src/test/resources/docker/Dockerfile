FROM mirror.gcr.io/library/cassandra:3.11

ENV CASSANDRA_YAML=/opt/cassandra/conf

COPY cassandra.yaml $CASSANDRA_YAML

RUN mkdir -p /var/lib/cassandra/data && \
    chown -R cassandra:cassandra $CASSANDRA_YAML/cassandra.yaml /var/lib/cassandra/data && \
    chmod 777 /var/lib/cassandra/data

USER cassandra

