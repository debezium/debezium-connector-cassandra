/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TupleType;

import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.AbstractTupleTypeDeserializer;

public class TupleTypeDeserializer extends AbstractTupleTypeDeserializer {

    public TupleTypeDeserializer(DebeziumTypeDeserializer deserializer) {
        super(deserializer, ProtocolConstants.DataType.TUPLE, TupleType.class);
    }

    @Override
    protected List<?> allTypes(Object abstractType) {
        return ((TupleType) abstractType).allTypes();
    }

    @Override
    protected ByteBuffer[] split(Object abstractType, ByteBuffer bb) {
        return ((TupleType) abstractType).split(bb);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object getAbstractTypeInstance(List<?> innerAbstractTypes) {
        return new TupleType((List<AbstractType<?>>) innerAbstractTypes);
    }

}
