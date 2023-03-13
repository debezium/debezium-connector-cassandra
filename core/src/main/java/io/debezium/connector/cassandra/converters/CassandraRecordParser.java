/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.converters;

import io.debezium.connector.cassandra.SourceInfo;
import io.debezium.converters.spi.RecordParser;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.Set;

/**
 * Parser for records produced by the Cassandra connector.
 *
 * @author Mario Fiore Vitale
 */
public class CassandraRecordParser extends RecordParser {

    private static final Set<String> CASSANDRA_SOURCE_FIELD = Collect.unmodifiableSet(
            SourceInfo.KEYSPACE_NAME_KEY,
            SourceInfo.TABLE_NAME_KEY);

    public CassandraRecordParser(Schema schema, Struct record) {
        super(schema, record, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public Object getMetadata(String name) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (CASSANDRA_SOURCE_FIELD.contains(name)) {
            return source().get(name);
        }

        throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from Cassandra connector");
    }
}
