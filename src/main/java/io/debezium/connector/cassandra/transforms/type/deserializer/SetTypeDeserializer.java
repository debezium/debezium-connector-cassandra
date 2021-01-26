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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class SetTypeDeserializer extends CollectionTypeDeserializer<SetType<?>> {

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        Set<?> deserializedSet = (Set<?>) super.deserialize(abstractType, bb);
        List<?> deserializedList = processElementsInDeserializedSet(abstractType, deserializedSet);
        return Values.convertToList(getSchemaBuilder(abstractType).build(), deserializedList);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        SetType<?> setType = (SetType<?>) abstractType;
        AbstractType<?> elementsType = setType.getElementsType();
        Schema innerSchema = CassandraTypeDeserializer.getSchemaBuilder(elementsType).build();
        return SchemaBuilder.array(innerSchema).optional();
    }

    @Override
    public Object deserialize(SetType<?> setType, ComplexColumnData ccd) {
        List<ByteBuffer> bbList = setType.serializedValues(ccd.iterator());
        AbstractType<?> elementsType = setType.getElementsType();
        Set<Object> deserializedSet = new HashSet<>();
        for (ByteBuffer bb : bbList) {
            deserializedSet.add(CassandraTypeDeserializer.deserialize(elementsType, bb));
        }
        List<Object> deserializedList = new ArrayList<>(deserializedSet);
        return Values.convertToList(getSchemaBuilder(setType).build(), deserializedList);
    }

    /**
     * Format or deserialize each elements in deserialized list:
     * If the element is logical type, format the element.
     * If the element is UserType or TupleType, deserialize the element.
     * @param abstractType the {@link AbstractType} of a column in Cassandra
     * @param deserializedSet Set deserialized from Cassandra
     * @return A deserialized list from Cassandra with each element that fits in it's Kafka Schema.
     */
    private List<Object> processElementsInDeserializedSet(AbstractType<?> abstractType, Set<?> deserializedSet) {
        AbstractType<?> elementsType = ((SetType<?>) abstractType).getElementsType();
        TypeDeserializer elementsTypeDeserializer = CassandraTypeDeserializer.getTypeDeserializer(elementsType);
        List<Object> resultedList;
        if (elementsTypeDeserializer instanceof LogicalTypeDeserializer) {
            resultedList = new ArrayList<>();
            for (Object element : deserializedSet) {
                Object convertedValue = ((LogicalTypeDeserializer) elementsTypeDeserializer).formatDeserializedValue(elementsType, element);
                resultedList.add(convertedValue);
            }
        }
        else if (elementsTypeDeserializer instanceof UserTypeDeserializer || elementsTypeDeserializer instanceof TupleTypeDeserializer) {
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
}
