/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;

import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.AbstractMapTypeDeserializer;

public class MapTypeDeserializer extends AbstractMapTypeDeserializer {

    public MapTypeDeserializer(DebeziumTypeDeserializer deserializer) {
        super(deserializer, ProtocolConstants.DataType.MAP, MapType.class);
    }

    @Override
    protected Object getKeysType(Object abstractType) {
        return ((MapType<?, ?>) abstractType).getKeysType();
    }

    @Override
    protected Object getValuesType(Object abstractType) {
        return ((MapType<?, ?>) abstractType).getValuesType();
    }

    @Override
    protected Object getAbstractTypeInstance(Object innerKeyAbstractType, Object innerValueAbstractType,
                                             boolean isMultiCell) {
        return MapType.getInstance((AbstractType<?>) innerKeyAbstractType, (AbstractType<?>) innerValueAbstractType, isMultiCell);
    }

}
