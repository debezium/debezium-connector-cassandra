/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public abstract class AbstractSetTypeDeserializer extends CollectionTypeDeserializer {

    public AbstractSetTypeDeserializer(DebeziumTypeDeserializer deserializer, Integer dataType, Class<?> abstractTypeClass) {
        super(deserializer, dataType, abstractTypeClass);
    }

    @Override
    public Object deserialize(Object abstractType, ByteBuffer bb) {
        Set<?> deserializedSet = (Set<?>) super.deserialize(abstractType, bb);
        List<?> deserializedList = processElementsInDeserializedSet(abstractType, deserializedSet);
        return Values.convertToList(getSchemaBuilder(abstractType).build(), deserializedList);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(Object abstractType) {
        Object elementsType = getElementsType(abstractType);
        Schema innerSchema = CassandraTypeDeserializer.getSchemaBuilder(elementsType).build();
        return SchemaBuilder.array(innerSchema).optional();
    }

    @Override
    public Object deserialize(Object abstractType, List<ByteBuffer> bbList) {
        Object elementsType = getElementsType(abstractType);
        Set<Object> deserializedSet = new HashSet<>();
        for (ByteBuffer bb : bbList) {
            deserializedSet.add(CassandraTypeDeserializer.deserialize(elementsType, bb));
        }
        List<Object> deserializedList = new ArrayList<>(deserializedSet);
        return Values.convertToList(getSchemaBuilder(abstractType).build(), deserializedList);
    }

    /**
     * Format or deserialize each elements in deserialized list:
     * If the element is logical type, format the element.
     * If the element is UserType or TupleType, deserialize the element.
     * @param abstractType the AbstractType of a column in Cassandra
     * @param deserializedSet Set deserialized from Cassandra
     * @return A deserialized list from Cassandra with each element that fits in it's Kafka Schema.
     */
    private List<Object> processElementsInDeserializedSet(Object abstractType, Set<?> deserializedSet) {
        Object elementsType = getElementsType(abstractType);
        TypeDeserializer elementsTypeDeserializer = CassandraTypeDeserializer.getTypeDeserializer(elementsType);
        List<Object> resultedList;
        if (elementsTypeDeserializer instanceof LogicalTypeDeserializer) {
            resultedList = new ArrayList<>();
            for (Object element : deserializedSet) {
                Object convertedValue = ((LogicalTypeDeserializer) elementsTypeDeserializer).formatDeserializedValue(elementsType, element);
                resultedList.add(convertedValue);
            }
        }
        else if (elementsTypeDeserializer instanceof AbstractUserDefinedTypeDeserializer || elementsTypeDeserializer instanceof AbstractTupleTypeDeserializer) {
            resultedList = new ArrayList<>();
            for (Object element : deserializedSet) {
                Object deserializedElement = elementsTypeDeserializer.deserialize(elementsType, (ByteBuffer) element);
                resultedList.add(deserializedElement);
            }
        }
        else {
            resultedList = new ArrayList<>(deserializedSet);
        }
        return resultedList;
    }

    @Override
    public Object getAbstractType(DataType dataType) {
        DefaultSetType setType = (DefaultSetType) dataType;
        DataType innerDataType = setType.getElementType();
        Object innerAbstractType = CassandraTypeDeserializer.getTypeDeserializer(innerDataType)
                .getAbstractType(innerDataType);
        return getAbstractTypeInstance(innerAbstractType, !setType.isFrozen());
    }

    protected abstract Object getElementsType(Object abstractType);

    protected abstract Object getAbstractTypeInstance(Object innerAbstractType, boolean isMultiCell);
}
