/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public class ListTypeDeserializer extends CollectionTypeDeserializer<ListType<?>> {

    private final DebeziumTypeDeserializer deserializer;

    public ListTypeDeserializer(DebeziumTypeDeserializer deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        List<?> deserializedList = (List<?>) deserializer.deserialize(abstractType, bb);
        deserializedList = processElementsInDeserializedList(abstractType, deserializedList);
        return Values.convertToList(getSchemaBuilder(abstractType).build(), deserializedList);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        ListType<?> listType = (ListType<?>) abstractType;
        AbstractType<?> elementsType = listType.getElementsType();
        Schema innerSchema = CassandraTypeDeserializer.getSchemaBuilder(elementsType).build();
        return SchemaBuilder.array(innerSchema).optional();
    }

    @Override
    public Object deserialize(ListType<?> listType, ComplexColumnData ccd) {
        List<ByteBuffer> bbList = listType.serializedValues(ccd.iterator());
        AbstractType<?> elementsType = listType.getElementsType();
        List<Object> deserializedList = new ArrayList<>(bbList.size());
        for (ByteBuffer bb : bbList) {
            deserializedList.add(CassandraTypeDeserializer.deserialize(elementsType, bb));
        }
        return Values.convertToList(getSchemaBuilder(listType).build(), deserializedList);
    }

    /**
     * Format or deserialize each elements in deserialized list:
     * If the element is logical type, format the element.
     * If the element is UserType or TupleType, deserialize the element.
     * @param abstractType the {@link AbstractType} of a column in Cassandra
     * @param deserializedList List deserialized from Cassandra
     * @return A deserialized list from Cassandra with each element that fits in it's Kafka Schema.
     */
    private List<?> processElementsInDeserializedList(AbstractType<?> abstractType, List<?> deserializedList) {
        AbstractType<?> elementsType = ((ListType<?>) abstractType).getElementsType();
        TypeDeserializer elementsTypeDeserializer = CassandraTypeDeserializer.getTypeDeserializer(elementsType);
        List<Object> resultedList;
        if (elementsTypeDeserializer instanceof LogicalTypeDeserializer) {
            resultedList = new ArrayList<>();
            for (Object element : deserializedList) {
                Object formattedElement = ((LogicalTypeDeserializer) elementsTypeDeserializer).formatDeserializedValue(elementsType, element);
                resultedList.add(formattedElement);
            }
        }
        else if (elementsTypeDeserializer instanceof UserDefinedTypeDeserializer || elementsTypeDeserializer instanceof TupleTypeDeserializer) {
            resultedList = new ArrayList<>();
            for (Object element : deserializedList) {
                Object deserializedElement = elementsTypeDeserializer.deserialize(elementsType, (ByteBuffer) element);
                resultedList.add(deserializedElement);
            }
        }
        else {
            resultedList = (List<Object>) deserializedList;
        }
        return resultedList;
    }
}
