/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type;

import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.marshal.DurationType;

import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.AbstractDurationTypeDeserializer;
import io.debezium.time.NanoDuration;

public class DurationTypeDeserializer extends AbstractDurationTypeDeserializer {

    public DurationTypeDeserializer(DebeziumTypeDeserializer deserializer) {
        super(deserializer, ProtocolConstants.DataType.DURATION, DurationType.instance);
    }

    @Override
    public Object formatDeserializedValue(Object abstractType, Object value) {
        Duration duration = (Duration) value;
        int months = duration.getMonths();
        int days = duration.getDays();
        long nanoSec = duration.getNanoseconds();
        return NanoDuration.durationNanos(0, months, days, 0, 0, 0, nanoSec);
    }

}
