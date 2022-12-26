/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public abstract class AbstractTupleTypeDeserializer extends AbstractTypeDeserializer {

    private static final String TUPLE_NAME_POSTFIX = "Tuple";
    private static final String FIELD_NAME_PREFIX = "field";

    public AbstractTupleTypeDeserializer(DebeziumTypeDeserializer deserializer, Integer dataType, Class<?> abstractTypeClass) {
        super(deserializer, dataType, abstractTypeClass);
    }

    @Override
    public Object deserialize(Object abstractType, ByteBuffer bb) {
        // the fun single case were we don't actually want to do the default deserialization!
        List<?> innerTypes = allTypes(abstractType);
        ByteBuffer[] innerValueByteBuffers = split(abstractType, bb);

        Struct struct = new Struct(getSchemaBuilder(abstractType).build());

        for (int i = 0; i < innerTypes.size(); i++) {
            Object currentInnerType = innerTypes.get(i);
            String fieldName = createFieldNameForIndex(i);
            Object deserializedInnerObject = CassandraTypeDeserializer.deserialize(currentInnerType, innerValueByteBuffers[i]);
            struct.put(fieldName, deserializedInnerObject);
        }

        return struct;
    }

    @Override
    public SchemaBuilder getSchemaBuilder(Object abstractType) {
        List<?> tupleInnerTypes = allTypes(abstractType);

        String recordName = createTupleName(tupleInnerTypes);

        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(recordName);

        for (int i = 0; i < tupleInnerTypes.size(); i++) {
            Object innerType = tupleInnerTypes.get(i);
            schemaBuilder.field(createFieldNameForIndex(i), CassandraTypeDeserializer.getSchemaBuilder(innerType).build());
        }

        return schemaBuilder.optional();
    }

    private String createTupleName(List<?> innerTypes) {
        StringBuilder tupleNameBuilder = new StringBuilder();
        for (Object innerType : innerTypes) {
            tupleNameBuilder.append(abstractTypeToNiceString(innerType));
        }
        return tupleNameBuilder.append(TUPLE_NAME_POSTFIX).toString();
    }

    private String createFieldNameForIndex(int i) {
        // begin indexing at 1
        return FIELD_NAME_PREFIX + (i + 1);
    }

    private String abstractTypeToNiceString(Object tupleInnerType) {
        // the full class name of the type. We want to pair it down to just the final type and remove the "Type".
        String typeName = tupleInnerType.getClass().getSimpleName();
        return typeName.substring(0, typeName.length() - 4);
    }

    @Override
    public Object getAbstractType(DataType dataType) {
        DefaultTupleType tupleType = (DefaultTupleType) dataType;
        List<DataType> innerTypes = tupleType.getComponentTypes();
        List<Object> innerAbstractTypes = new ArrayList<>(innerTypes.size());
        for (DataType dt : innerTypes) {
            Object innerAbstractType = CassandraTypeDeserializer.getTypeDeserializer(dt).getAbstractType(dt);
            innerAbstractTypes.add(innerAbstractType);
        }
        return getAbstractTypeInstance(innerAbstractTypes);
    }

    protected abstract List<?> allTypes(Object abstractType);

    protected abstract ByteBuffer[] split(Object abstractType, ByteBuffer bb);

    protected abstract Object getAbstractTypeInstance(List<?> innerAbstractTypes);
}
