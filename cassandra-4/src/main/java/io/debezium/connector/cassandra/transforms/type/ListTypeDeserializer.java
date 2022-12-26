/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;

import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.AbstractListTypeDeserializer;

public class ListTypeDeserializer extends AbstractListTypeDeserializer {

    public ListTypeDeserializer(DebeziumTypeDeserializer deserializer) {
        super(deserializer, ProtocolConstants.DataType.LIST, ListType.class);
    }

    @Override
    protected Object getElementsType(Object abstractType) {
        return ((ListType<?>) abstractType).getElementsType();
    }

    @Override
    protected Object getAbstractTypeInstance(Object innerAbstractType, boolean isMultiCell) {
        return ListType.getInstance((AbstractType<?>) innerAbstractType, isMultiCell);
    }

}
