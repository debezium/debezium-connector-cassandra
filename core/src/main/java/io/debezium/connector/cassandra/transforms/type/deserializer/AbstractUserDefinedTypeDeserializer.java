/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public abstract class AbstractUserDefinedTypeDeserializer extends AbstractTypeDeserializer {

    public AbstractUserDefinedTypeDeserializer(DebeziumTypeDeserializer deserializer, Integer dataType, Class<?> abstractTypeClass) {
        super(deserializer, dataType, abstractTypeClass);
    }

    public Object deserialize(Object abstractType, ByteBuffer bb) {
        List<ByteBuffer> elements = bbList(abstractType, bb);
        Struct struct = new Struct(getSchemaBuilder(abstractType).build());
        List<String> fieldNames = fieldNames(abstractType);
        List<?> fieldTypes = fieldTypes(abstractType);
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            Object fieldType = fieldTypes.get(i);
            struct.put(fieldName, CassandraTypeDeserializer.deserialize(fieldType, elements.get(i)));
        }
        return struct;
    }

    @Override
    public SchemaBuilder getSchemaBuilder(Object abstractType) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(structName(abstractType));
        List<String> fieldNames = fieldNames(abstractType);
        List<?> fieldTypes = fieldTypes(abstractType);
        for (int i = 0; i < fieldNames.size(); i++) {
            Schema fieldSchema = CassandraTypeDeserializer.getSchemaBuilder(fieldTypes.get(i)).build();
            schemaBuilder.field(fieldNames.get(i), fieldSchema);
        }
        return schemaBuilder.optional();
    }

    @Override
    public Object getAbstractType(DataType dataType) {
        DefaultUserDefinedType userType = (DefaultUserDefinedType) dataType;
        List<String> fieldNames = userType.getFieldNames()
                .stream()
                .map(CqlIdentifier::toString)
                .collect(Collectors.toList());
        List<DataType> fieldTypes = userType.getFieldTypes();
        List<Object> innerAbstractTypes = new ArrayList<>(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            DataType innerDataType = fieldTypes.get(i);
            Object innerAbstractType = CassandraTypeDeserializer.getTypeDeserializer(innerDataType)
                    .getAbstractType(innerDataType);
            innerAbstractTypes.add(innerAbstractType);
        }
        return getAbstractTypeInstance(userType, fieldNames, innerAbstractTypes);
    }

    protected abstract List<String> fieldNames(Object abstractType);

    protected abstract List<?> fieldTypes(Object abstractType);

    protected abstract List<ByteBuffer> bbList(Object abstractType, ByteBuffer bb);

    protected abstract String structName(Object abstractType);

    protected abstract Object getAbstractTypeInstance(DefaultUserDefinedType userType, List<String> fieldNames,
                                                      List<?> innerAbstractTypes);
}
