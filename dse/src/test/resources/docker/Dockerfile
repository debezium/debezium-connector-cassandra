FROM mirror.gcr.io/datastax/dse-server:6.8.16

ENV DS_LICENSE=accept
ENV DSE_AUTO_CONF_OFF=all

COPY --chown=dse:dse cassandra.yaml /opt/dse/resources/cassandra/conf

USER dse
