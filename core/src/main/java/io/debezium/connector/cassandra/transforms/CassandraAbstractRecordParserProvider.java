/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.cassandra.converters.CassandraRecordParser;
import io.debezium.converters.spi.RecordParser;
import io.debezium.transforms.spi.RecordParserProvider;

public abstract class CassandraAbstractRecordParserProvider implements RecordParserProvider {

    @Override
    public String getName() {
        return "cassandra";
    }

    @Override
    public RecordParser createParser(Schema schema, Struct record) {
        return new CassandraRecordParser(schema, record);
    }
}
