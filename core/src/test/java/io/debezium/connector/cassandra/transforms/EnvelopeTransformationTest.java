/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.connector.cassandra.Record;
import io.debezium.data.Envelope;

class EnvelopeTransformationTest {

    @Test
    void transformOperationInsert() {
        testTransformOperation(Record.Operation.INSERT, Envelope.Operation.CREATE);
    }

    @Test
    void transformOperationUpdate() {
        testTransformOperation(Record.Operation.UPDATE, Envelope.Operation.UPDATE);
    }

    @Test
    void transformOperationDelete() {
        testTransformOperation(Record.Operation.DELETE, Envelope.Operation.DELETE);
    }

    @Test
    void transformOperationRangeTombostone() {
        testTransformOperation(Record.Operation.RANGE_TOMBSTONE, Envelope.Operation.TRUNCATE);
    }

    private void testTransformOperation(Record.Operation from, Envelope.Operation to) {
        SourceRecord sourceRecord = generateSourceRecord(from);
        assertEquals(((Struct) sourceRecord.value()).get("op"), from.getValue());
        EnvelopeTransformation<SourceRecord> transform = new EnvelopeTransformation<>();
        transform.configure(Collections.emptyMap());
        SourceRecord transformedRecord = transform.apply(sourceRecord);
        assertEquals(((Struct) transformedRecord.value()).get("op"), to.code());
        transform.close();
    }

    private SourceRecord generateSourceRecord(Record.Operation op) {
        final Schema recordSchema = SchemaBuilder.struct().field("op", SchemaBuilder.string()).build();
        Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(SchemaBuilder.struct().build())
                .build();
        final Struct payload = new Struct(recordSchema);
        payload.put("op", op.getValue());
        final SourceRecord sourceRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "test_topic",
                SchemaBuilder.STRING_SCHEMA,
                "key",
                envelope.schema(),
                payload);
        return sourceRecord;
    }

}
