/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.Record;
import io.debezium.data.Envelope;
import io.debezium.transforms.SmtManager;

/**
 * This SMT allows the Cassandra connector to emit events in accordance with {@link Envelope}.
 */
public class EnvelopeTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

    private SmtManager<R> smtManager;

    @Override
    public R apply(R record) {
        if (record.value() == null || !smtManager.isValidEnvelope(record)) {
            return record;
        }

        Struct originalValueStruct = (Struct) record.value();
        Struct updatedValueStruct;
        String operation = originalValueStruct.getString(Envelope.FieldName.OPERATION);

        Envelope.Operation op;
        if (Record.Operation.INSERT.getValue().equals(operation)) {
            op = Envelope.Operation.CREATE;
        }
        else if (Record.Operation.UPDATE.getValue().equals(operation)) {
            op = Envelope.Operation.UPDATE;
        }
        else if (Record.Operation.DELETE.getValue().equals(operation)) {
            op = Envelope.Operation.DELETE;
        }
        else if (Record.Operation.RANGE_TOMBSTONE.getValue().equals(operation)) {
            op = Envelope.Operation.TRUNCATE;
        }
        else {
            return record;
        }

        updatedValueStruct = originalValueStruct.put(Envelope.FieldName.OPERATION, op.code());

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                updatedValueStruct,
                record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> props) {
        final Configuration config = Configuration.from(props);
        smtManager = new SmtManager<>(config);
    }
}
