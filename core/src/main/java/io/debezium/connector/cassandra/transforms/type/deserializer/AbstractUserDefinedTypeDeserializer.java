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

import org.apache.kafka.connect.data.Field;
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
            // A serialized value may hold fewer components than the type has fields, e.g. when it was
            // written before fields were added with ALTER TYPE ... ADD; such trailing fields are null
            final var element = i < elements.size() ? elements.get(i) : null;
            final var value = CassandraTypeDeserializer.deserialize(fieldType, element);
            struct.put(fieldName, withSchema(value, struct.schema().field(fieldName).schema()));
        }
        return struct;
    }

    @Override
    public SchemaBuilder getSchemaBuilder(Object abstractType) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(structName(abstractType));
        List<String> fieldNames = fieldNames(abstractType);
        List<?> fieldTypes = fieldTypes(abstractType);
        for (int i = 0; i < fieldNames.size(); i++) {
            // every field is optional, as a UDT value may lack any of its fields
            Schema fieldSchema = CassandraTypeDeserializer.getSchemaBuilder(fieldTypes.get(i)).optional().build();
            schemaBuilder.field(fieldNames.get(i), fieldSchema);
        }
        return schemaBuilder.optional();
    }

    /**
     * Rebinds a struct value, e.g. a precise decimal, created with a required schema to the optional field schema.
     */
    private static Object withSchema(Object value, Schema schema) {
        if (value instanceof Struct valueStruct && !valueStruct.schema().equals(schema)) {
            final var struct = new Struct(schema);
            for (Field field : schema.fields()) {
                struct.put(field, valueStruct.get(field.name()));
            }
            return struct;
        }
        return value;
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
