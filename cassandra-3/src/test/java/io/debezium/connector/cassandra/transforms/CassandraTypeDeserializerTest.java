/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;

import io.debezium.connector.cassandra.Cassandra3TypeProvider;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer.DecimalMode;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer.VarIntMode;

class CassandraTypeDeserializerTest {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    @BeforeAll
    static void beforeAll() {
        Cassandra3TypeProvider provider = new Cassandra3TypeProvider();
        CassandraTypeDeserializer.init(provider.deserializers(), DecimalMode.DOUBLE, VarIntMode.LONG,
                provider.baseTypeForReversedType());
    }

    @Test
    void testAscii() {
        String expectedAscii = "some text";

        ByteBuffer serializedAscii = AsciiType.instance.decompose(expectedAscii);

        Object deserializedAscii = CassandraTypeDeserializer.deserialize(AsciiType.instance, serializedAscii);
        assertEquals("some text", deserializedAscii);

        deserializedAscii = CassandraTypeDeserializer.deserialize(DataTypes.ASCII, serializedAscii);
        assertEquals("some text", deserializedAscii);
    }

    @Test
    void testBoolean() {
        Boolean expectedBoolean = true;

        ByteBuffer serializedBoolean = BooleanType.instance.decompose(expectedBoolean);

        Object deserializedBoolean = CassandraTypeDeserializer.deserialize(BooleanType.instance, serializedBoolean);
        assertEquals(expectedBoolean, deserializedBoolean);

        deserializedBoolean = CassandraTypeDeserializer.deserialize(DataTypes.BOOLEAN, serializedBoolean);
        assertEquals(expectedBoolean, deserializedBoolean);
    }

    @Test
    void testBytes() {
        ByteBuffer expectedBytes = ByteBuffer.wrap("some random stuff here".getBytes(CHARSET));

        // Pretty sure this is a no-op, but for consistency...
        ByteBuffer serializedBytes = BytesType.instance.decompose(expectedBytes);

        Object deserializedBytes = CassandraTypeDeserializer.deserialize(BytesType.instance, serializedBytes);
        assertEquals(expectedBytes, deserializedBytes);

        deserializedBytes = CassandraTypeDeserializer.deserialize(DataTypes.BLOB, serializedBytes);
        assertEquals(expectedBytes, deserializedBytes);
    }

    @Test
    void testByteType() {
        Byte expectedByte = Byte.valueOf("9");

        ByteBuffer serializedByte = ByteType.instance.decompose(expectedByte);

        Object deserializedByte = CassandraTypeDeserializer.deserialize(ByteType.instance, serializedByte);
        assertEquals(expectedByte, deserializedByte);

        deserializedByte = CassandraTypeDeserializer.deserialize(DataTypes.TINYINT, serializedByte);
        assertEquals(expectedByte, deserializedByte);
    }

    @Test
    void testCounterColumnType() {
        Long expectedCounterColumnType = 42L;

        ByteBuffer serializedCounter = CounterColumnType.instance.decompose(42L);

        Object deserializedCounter = CassandraTypeDeserializer.deserialize(CounterColumnType.instance, serializedCounter);
        assertEquals(expectedCounterColumnType, deserializedCounter);

        deserializedCounter = CassandraTypeDeserializer.deserialize(DataTypes.COUNTER, serializedCounter);
        assertEquals(expectedCounterColumnType, deserializedCounter);
    }

    @Test
    void testDecimalType() {
        BigDecimal expectedDecimal = BigDecimal.valueOf(Math.PI);

        ByteBuffer serializedDecimal = DecimalType.instance.decompose(expectedDecimal);

        // decimal.handling.mode = DOUBLE (default)
        Object deserializedDecimalAsDouble = CassandraTypeDeserializer.deserialize(DecimalType.instance, serializedDecimal);
        assertEquals(expectedDecimal.doubleValue(), deserializedDecimalAsDouble);

        deserializedDecimalAsDouble = CassandraTypeDeserializer.deserialize(DataTypes.DECIMAL, serializedDecimal);
        assertEquals(expectedDecimal.doubleValue(), deserializedDecimalAsDouble);

        // decimal.handling.mode = PRECISE
        CassandraTypeDeserializer.setDecimalMode(DecimalMode.PRECISE);
        Object deserializedDecimalAsStruct = CassandraTypeDeserializer.deserialize(DecimalType.instance, serializedDecimal);
        Schema decimalSchema = CassandraTypeDeserializer.getSchemaBuilder(DecimalType.instance).build();
        Struct expectedDecimalStruct = new Struct(decimalSchema)
                .put("value", expectedDecimal.unscaledValue().toByteArray())
                .put("scale", expectedDecimal.scale());
        assertEquals(expectedDecimalStruct, deserializedDecimalAsStruct);

        deserializedDecimalAsStruct = CassandraTypeDeserializer.deserialize(DataTypes.DECIMAL, serializedDecimal);
        assertEquals(expectedDecimalStruct, deserializedDecimalAsStruct);

        // decimal.handling.mode = STRING
        CassandraTypeDeserializer.setDecimalMode(DecimalMode.STRING);
        Object deserializedDecimalAsString = CassandraTypeDeserializer.deserialize(DecimalType.instance, serializedDecimal);
        assertEquals(expectedDecimal.toPlainString(), deserializedDecimalAsString);

        deserializedDecimalAsString = CassandraTypeDeserializer.deserialize(DataTypes.DECIMAL, serializedDecimal);
        assertEquals(expectedDecimal.toPlainString(), deserializedDecimalAsString);
    }

    @Test
    void testDoubleType() {
        Double expectedDouble = 100.5;

        ByteBuffer serializedDouble = DoubleType.instance.decompose(expectedDouble);

        Object deserializedDouble = CassandraTypeDeserializer.deserialize(DoubleType.instance, serializedDouble);
        assertEquals(expectedDouble, deserializedDouble);

        deserializedDouble = CassandraTypeDeserializer.deserialize(DataTypes.DOUBLE, serializedDouble);
        assertEquals(expectedDouble, deserializedDouble);
    }

    @Test
    void testDurationType() {
        Duration sourceDuration = Duration.newInstance(1, 3, 500);

        long expectedNanoDuration = (30 + 3) * ChronoUnit.DAYS.getDuration().toNanos() + 500;

        ByteBuffer serializedDuration = DurationType.instance.decompose(sourceDuration);

        Object deserializedDuration = CassandraTypeDeserializer.deserialize(DurationType.instance, serializedDuration);
        assertEquals(expectedNanoDuration, deserializedDuration);

        deserializedDuration = CassandraTypeDeserializer.deserialize(DataTypes.DURATION, serializedDuration);
        assertEquals(expectedNanoDuration, deserializedDuration);
    }

    @Test
    void testFloatType() {
        Float expectedFloat = 66.6F;

        ByteBuffer serializedFloat = FloatType.instance.decompose(expectedFloat);

        Object deserializedFloat = CassandraTypeDeserializer.deserialize(FloatType.instance, serializedFloat);
        assertEquals(expectedFloat, deserializedFloat);

        deserializedFloat = CassandraTypeDeserializer.deserialize(DataTypes.FLOAT, serializedFloat);
        assertEquals(expectedFloat, deserializedFloat);
    }

    @Test
    void testInetAddressType() throws UnknownHostException {
        InetAddress sourceInetAddress = InetAddress.getLocalHost();
        // the address is the only thing that cassandra will seralize for an inetadress.
        String expectedInetAddress = "/" + sourceInetAddress.getHostAddress();

        ByteBuffer serializedInetAddress = InetAddressType.instance.decompose(sourceInetAddress);

        Object deserializedInetAddress = CassandraTypeDeserializer.deserialize(InetAddressType.instance, serializedInetAddress);
        assertEquals(expectedInetAddress, deserializedInetAddress);

        deserializedInetAddress = CassandraTypeDeserializer.deserialize(DataTypes.INET, serializedInetAddress);
        assertEquals(expectedInetAddress, deserializedInetAddress);
    }

    @Test
    void testInt32Type() {
        Integer expectedInteger = 8;

        ByteBuffer serializedInt32 = Int32Type.instance.decompose(expectedInteger);

        Object deserializedInt32 = CassandraTypeDeserializer.deserialize(Int32Type.instance, serializedInt32);
        assertEquals(expectedInteger, deserializedInt32);

        deserializedInt32 = CassandraTypeDeserializer.deserialize(DataTypes.INT, serializedInt32);
        assertEquals(expectedInteger, deserializedInt32);
    }

    @Test
    void testIntegerType() {
        BigInteger expectedInteger = BigInteger.valueOf(8);

        ByteBuffer serializedVarInt = IntegerType.instance.decompose(expectedInteger);

        // varint.handling.mode = LONG (default)
        Object deserializedVarIntAsLong = CassandraTypeDeserializer.deserialize(IntegerType.instance, serializedVarInt);
        assertEquals(expectedInteger.longValue(), deserializedVarIntAsLong);

        deserializedVarIntAsLong = CassandraTypeDeserializer.deserialize(DataTypes.VARINT, serializedVarInt);
        assertEquals(expectedInteger.longValue(), deserializedVarIntAsLong);

        // varint.handling.mode = PRECISE
        CassandraTypeDeserializer.setVarIntMode(VarIntMode.PRECISE);
        Object deserializedVarIntAsBigDecimal = CassandraTypeDeserializer.deserialize(IntegerType.instance, serializedVarInt);
        assertEquals(new BigDecimal(expectedInteger), deserializedVarIntAsBigDecimal);

        deserializedVarIntAsBigDecimal = CassandraTypeDeserializer.deserialize(DataTypes.VARINT, serializedVarInt);
        assertEquals(new BigDecimal(expectedInteger), deserializedVarIntAsBigDecimal);

        // varint.handling.mode = STRING
        CassandraTypeDeserializer.setVarIntMode(VarIntMode.STRING);
        Object deserializedVarIntAsString = CassandraTypeDeserializer.deserialize(IntegerType.instance, serializedVarInt);
        assertEquals(expectedInteger.toString(), deserializedVarIntAsString);

        deserializedVarIntAsString = CassandraTypeDeserializer.deserialize(DataTypes.VARINT, serializedVarInt);
        assertEquals(expectedInteger.toString(), deserializedVarIntAsString);
    }

    @Test
    void testListType() {
        List<Integer> expectedList = new ArrayList<>();
        expectedList.add(1);
        expectedList.add(3);
        expectedList.add(5);

        // non-frozen
        ListType<Integer> nonFrozenListType = ListType.getInstance(Int32Type.instance, true);
        ByteBuffer serializedList = nonFrozenListType.decompose(expectedList);
        Object deserializedList = CassandraTypeDeserializer.deserialize(nonFrozenListType, serializedList);
        assertEquals(expectedList, deserializedList);

        deserializedList = CassandraTypeDeserializer.deserialize(DataTypes.listOf(DataTypes.INT), serializedList);
        assertEquals(expectedList, deserializedList);

        // frozen
        ListType<Integer> frozenListType = ListType.getInstance(Int32Type.instance, false);
        serializedList = frozenListType.decompose(expectedList);
        deserializedList = CassandraTypeDeserializer.deserialize(frozenListType, serializedList);
        assertEquals(expectedList, deserializedList);

        deserializedList = CassandraTypeDeserializer.deserialize(DataTypes.frozenListOf(DataTypes.INT), serializedList);
        assertEquals(expectedList, deserializedList);
    }

    @Test
    void testLongType() {
        Long expectedLong = 8L;

        ByteBuffer serializedLong = LongType.instance.decompose(expectedLong);

        Object deserializedLong = CassandraTypeDeserializer.deserialize(LongType.instance, serializedLong);
        assertEquals(expectedLong, deserializedLong);

        deserializedLong = CassandraTypeDeserializer.deserialize(DataTypes.BIGINT, serializedLong);
        assertEquals(expectedLong, deserializedLong);
    }

    @Test
    void testMapType() {
        Map<String, Double> expectedMap = new HashMap<>();
        expectedMap.put("foo", 1D);
        expectedMap.put("bar", 50D);

        // non-frozen
        MapType<String, Double> nonFrozenMapType = MapType.getInstance(AsciiType.instance, DoubleType.instance, true);
        ByteBuffer serializedMap = nonFrozenMapType.decompose(expectedMap);
        Object deserializedMap = CassandraTypeDeserializer.deserialize(nonFrozenMapType, serializedMap);
        assertEquals(expectedMap, deserializedMap);

        deserializedMap = CassandraTypeDeserializer.deserialize(DataTypes.mapOf(DataTypes.ASCII, DataTypes.DOUBLE), serializedMap);
        assertEquals(expectedMap, deserializedMap);

        // frozen
        MapType<String, Double> frozenMapType = MapType.getInstance(AsciiType.instance, DoubleType.instance, false);
        serializedMap = frozenMapType.decompose(expectedMap);
        deserializedMap = CassandraTypeDeserializer.deserialize(frozenMapType, serializedMap);
        assertEquals(expectedMap, deserializedMap);

        deserializedMap = CassandraTypeDeserializer.deserialize(DataTypes.frozenMapOf(DataTypes.ASCII, DataTypes.DOUBLE), serializedMap);
        assertEquals(expectedMap, deserializedMap);
    }

    @Test
    void testMapTypeNonStringKeys() {
        Map<Integer, Float> sourceMap = new HashMap<>();
        sourceMap.put(1, 1.5F);
        sourceMap.put(2, 3.1414F);

        Map<Integer, Float> expectedMap = new HashMap<>();
        expectedMap.put(1, 1.5F);
        expectedMap.put(2, 3.1414F);

        MapType<Integer, Float> mapType = MapType.getInstance(Int32Type.instance, FloatType.instance, true);
        ByteBuffer serializedMap = mapType.decompose(sourceMap);
        Object deserializedMap = CassandraTypeDeserializer.deserialize(mapType, serializedMap);
        assertEquals(expectedMap, deserializedMap);

        deserializedMap = CassandraTypeDeserializer.deserialize(DataTypes.mapOf(DataTypes.INT, DataTypes.FLOAT), serializedMap);
        assertEquals(expectedMap, deserializedMap);
    }

    @Test
    void testSetType() {
        Set<Float> sourceSet = new HashSet<>();
        sourceSet.add(42F);
        sourceSet.add(123F);

        // non-frozen
        SetType<Float> nonFrozenSetType = SetType.getInstance(FloatType.instance, true);
        ByteBuffer serializedSet = nonFrozenSetType.decompose(sourceSet);
        Collection<?> deserializedSet = (Collection<?>) CassandraTypeDeserializer.deserialize(nonFrozenSetType, serializedSet);
        // order may be different in the resulting collection.
        assertTrue(sourceSet.containsAll(deserializedSet));
        assertTrue(deserializedSet.containsAll(sourceSet));

        deserializedSet = (Collection<?>) CassandraTypeDeserializer.deserialize(DataTypes.setOf(DataTypes.FLOAT), serializedSet);
        assertTrue(sourceSet.containsAll(deserializedSet));
        assertTrue(deserializedSet.containsAll(sourceSet));

        // frozen
        SetType<Float> frozenSetType = SetType.getInstance(FloatType.instance, false);
        serializedSet = frozenSetType.decompose(sourceSet);
        deserializedSet = (Collection<?>) CassandraTypeDeserializer.deserialize(frozenSetType, serializedSet);
        assertTrue(sourceSet.containsAll(deserializedSet));
        assertTrue(deserializedSet.containsAll(sourceSet));

        deserializedSet = (Collection<?>) CassandraTypeDeserializer.deserialize(DataTypes.frozenSetOf(DataTypes.FLOAT), serializedSet);
        assertTrue(sourceSet.containsAll(deserializedSet));
        assertTrue(deserializedSet.containsAll(sourceSet));
    }

    @Test
    void testShortType() {
        Short expectedShort = (short) 2;

        ByteBuffer serializedShort = ShortType.instance.decompose(expectedShort);

        Object deserializedShort = CassandraTypeDeserializer.deserialize(ShortType.instance, serializedShort);
        assertEquals(expectedShort, deserializedShort);

        deserializedShort = CassandraTypeDeserializer.deserialize(DataTypes.SMALLINT, serializedShort);
        assertEquals(expectedShort, deserializedShort);
    }

    @Test
    void testSimpleDateType() {
        String dateString = "2019-02-19";
        Integer expectedDaysSinceEpoch = 17946;

        ByteBuffer serializedDate = SimpleDateType.instance.fromString(dateString);

        Object deserializedDate = CassandraTypeDeserializer.deserialize(SimpleDateType.instance, serializedDate);
        assertEquals(expectedDaysSinceEpoch, deserializedDate);

        deserializedDate = CassandraTypeDeserializer.deserialize(DataTypes.DATE, serializedDate);
        assertEquals(expectedDaysSinceEpoch, deserializedDate);
    }

    @Test
    void testTimeType() {
        Long expectedTime = 30L;

        ByteBuffer serializedTime = TimeType.instance.decompose(expectedTime);

        Object deserializedTime = CassandraTypeDeserializer.deserialize(TimeType.instance, serializedTime);
        assertEquals(expectedTime, deserializedTime);

        deserializedTime = CassandraTypeDeserializer.deserialize(DataTypes.TIME, serializedTime);
        assertEquals(expectedTime, deserializedTime);
    }

    @Test
    void testTimestampType() {
        Date timestamp = new Date();
        Long expectedLongTimestamp = timestamp.getTime();

        ByteBuffer serializedTimestamp = TimestampType.instance.decompose(timestamp);

        Object deserializedTimestamp = CassandraTypeDeserializer.deserialize(TimestampType.instance, serializedTimestamp);
        assertEquals(expectedLongTimestamp, deserializedTimestamp);

        deserializedTimestamp = CassandraTypeDeserializer.deserialize(DataTypes.TIMESTAMP, serializedTimestamp);
        assertEquals(expectedLongTimestamp, deserializedTimestamp);
    }

    @Test
    void testTimeUUIDType() {
        UUID timeUUID = UUID.randomUUID();

        ByteBuffer serializedTimeUUID = TimeUUIDType.instance.decompose(timeUUID);

        Object deserializedTimeUUID = CassandraTypeDeserializer.deserialize(TimeUUIDType.instance, serializedTimeUUID);
        assertEquals(timeUUID.toString(), deserializedTimeUUID);

        deserializedTimeUUID = CassandraTypeDeserializer.deserialize(DataTypes.TIMEUUID, serializedTimeUUID);
        assertEquals(timeUUID.toString(), deserializedTimeUUID);
    }

    @Test
    void testTupleType() {
        List<AbstractType<?>> innerAbstractTypes = new ArrayList<>(2);
        innerAbstractTypes.add(AsciiType.instance);
        innerAbstractTypes.add(ShortType.instance);
        TupleType tupleType = new TupleType(innerAbstractTypes);

        String sourceTupleString = "foo:1";
        ByteBuffer serializedTuple = tupleType.fromString(sourceTupleString);

        Schema tupleSchema = CassandraTypeDeserializer.getSchemaBuilder(tupleType).build();
        Struct expectedTuple = new Struct(tupleSchema)
                .put("field1", "foo")
                .put("field2", (short) 1);

        Object deserializedTuple = CassandraTypeDeserializer.deserialize(tupleType, serializedTuple);
        assertEquals(expectedTuple, deserializedTuple);

        deserializedTuple = CassandraTypeDeserializer.deserialize(DataTypes.tupleOf(DataTypes.ASCII, DataTypes.SMALLINT), serializedTuple);
        assertEquals(expectedTuple, deserializedTuple);
    }

    @Test
    void testUserType() {
        // this is slightly complicated, so we're testing in two parts:
        // first, explicitly test for schema correctness
        ByteBuffer expectedTypeName = ByteBuffer.wrap("FooType".getBytes(Charset.defaultCharset()));
        List<FieldIdentifier> expectedFieldIdentifiers = new ArrayList<>();
        expectedFieldIdentifiers.add(new FieldIdentifier(ByteBuffer.wrap("asciiField".getBytes(Charset.defaultCharset()))));
        expectedFieldIdentifiers.add(new FieldIdentifier(ByteBuffer.wrap("doubleField".getBytes(Charset.defaultCharset()))));
        expectedFieldIdentifiers.add(new FieldIdentifier(ByteBuffer.wrap("durationField".getBytes(Charset.defaultCharset()))));
        // testing duration to make sure that recursive deserialization works correctly
        List<AbstractType<?>> expectedFieldTypes = new ArrayList<>();
        expectedFieldTypes.add(AsciiType.instance);
        expectedFieldTypes.add(DoubleType.instance);
        expectedFieldTypes.add(DurationType.instance);
        UserType userType = new UserType("barspace",
                expectedTypeName,
                expectedFieldIdentifiers,
                expectedFieldTypes,
                true);

        Schema userSchema = CassandraTypeDeserializer.getSchemaBuilder(userType).build();

        long expectedNanoDuration = (30 + 2) * ChronoUnit.DAYS.getDuration().toNanos() + 3;

        Struct expectedUserTypeData = new Struct(userSchema)
                .put("asciiField", "foobar")
                .put("doubleField", 1.5d)
                .put("durationField", expectedNanoDuration);

        Map<String, Object> jsonObject = new HashMap<>(3);
        jsonObject.put("\"asciiField\"", "foobar");
        jsonObject.put("\"doubleField\"", 1.5d);
        jsonObject.put("\"durationField\"", DurationType.instance.getSerializer().toString(Duration.newInstance(1, 2, 3)));
        Term userTypeObject = userType.fromJSONObject(jsonObject);

        ByteBuffer buffer = userTypeObject.bindAndGet(QueryOptions.DEFAULT);

        ByteBuffer serializedUserTypeObject = userType.decompose(buffer);

        Object deserializedUserTypeObject = CassandraTypeDeserializer.deserialize(userType, serializedUserTypeObject);
        assertEquals(expectedUserTypeData, deserializedUserTypeObject);

        DefaultUserDefinedType userDefinedType = new DefaultUserDefinedType(CqlIdentifier.fromCql("\"barspace\""),
                CqlIdentifier.fromCql("\"FooType\""), false,
                Arrays.asList(CqlIdentifier.fromCql("\"asciiField\""), CqlIdentifier.fromCql("\"doubleField\""), CqlIdentifier.fromCql("\"durationField\"")),
                Arrays.asList(DataTypes.ASCII, DataTypes.DOUBLE, DataTypes.DURATION));
        deserializedUserTypeObject = CassandraTypeDeserializer.deserialize(userDefinedType, serializedUserTypeObject);
        assertEquals(expectedUserTypeData, deserializedUserTypeObject);
    }

    @Test
    void testUTF8Type() {
        String expectedUTF8 = "Fourscore and seven years ago";

        ByteBuffer serializedUTF8 = UTF8Type.instance.decompose(expectedUTF8);

        Object deserializedUTF8 = CassandraTypeDeserializer.deserialize(UTF8Type.instance, serializedUTF8);
        assertEquals(expectedUTF8, deserializedUTF8);

        deserializedUTF8 = CassandraTypeDeserializer.deserialize(DataTypes.TEXT, serializedUTF8);
        assertEquals(expectedUTF8, deserializedUTF8);
    }

    @Test
    void testUUIDType() {
        UUID uuid = UUID.randomUUID();

        String expectedFixedUUID = uuid.toString();

        ByteBuffer serializedUUID = UUIDType.instance.decompose(uuid);

        Object deserializedUUID = CassandraTypeDeserializer.deserialize(UUIDType.instance, serializedUUID);
        assertEquals(expectedFixedUUID, deserializedUUID);

        deserializedUUID = CassandraTypeDeserializer.deserialize(DataTypes.UUID, serializedUUID);
        assertEquals(expectedFixedUUID, deserializedUUID);
    }

    @Test
    void testReversedType() {
        Date timestamp = new Date();
        Long expectedLongTimestamp = timestamp.getTime();

        ByteBuffer serializedTimestamp = TimestampType.instance.decompose(timestamp);

        AbstractType<?> reversedTimeStampType = ReversedType.getInstance(TimestampType.instance);

        Object deserializedTimestamp = CassandraTypeDeserializer.deserialize(reversedTimeStampType, serializedTimestamp);
        assertEquals(expectedLongTimestamp, deserializedTimestamp);
    }

    @Test
    void testListUUIDType() {

        List<UUID> originalList = new ArrayList<>();
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();
        originalList.add(uuid1);
        originalList.add(uuid2);
        originalList.add(uuid3);

        List<String> expectedList = new ArrayList<>();
        String expectedUuidStr1 = uuid1.toString();
        String expectedUuidStr2 = uuid2.toString();
        String expectedUuidStr3 = uuid3.toString();
        expectedList.add(expectedUuidStr1);
        expectedList.add(expectedUuidStr2);
        expectedList.add(expectedUuidStr3);

        ListType<UUID> frozenListType = ListType.getInstance(UUIDType.instance, false);
        ByteBuffer serializedList = frozenListType.decompose(originalList);
        Object deserializedList = CassandraTypeDeserializer.deserialize(frozenListType, serializedList);
        assertEquals(expectedList, deserializedList);

        deserializedList = CassandraTypeDeserializer.deserialize(DataTypes.listOf(DataTypes.UUID), serializedList);
        assertEquals(expectedList, deserializedList);
    }

    @Test
    void testListUserType() {

        ByteBuffer userTypeName = ByteBuffer.wrap("FooType".getBytes(Charset.defaultCharset()));
        List<FieldIdentifier> userTypeFieldIdentifiers = new ArrayList<>();
        userTypeFieldIdentifiers.add(new FieldIdentifier(ByteBuffer.wrap("asciiField".getBytes(Charset.defaultCharset()))));
        userTypeFieldIdentifiers.add(new FieldIdentifier(ByteBuffer.wrap("setField".getBytes(Charset.defaultCharset()))));
        SetType<String> frozenSetType = SetType.getInstance(AsciiType.instance, false);
        List<AbstractType<?>> userFieldTypes = new ArrayList<>();
        userFieldTypes.add(AsciiType.instance);
        userFieldTypes.add(frozenSetType);
        UserType userType = new UserType("barspace",
                userTypeName,
                userTypeFieldIdentifiers,
                userFieldTypes,
                false);

        Schema userTypeSchema = CassandraTypeDeserializer.getSchemaBuilder(userType).build();
        Set<String> sourceSet = new HashSet<>();
        sourceSet.add("text1");
        sourceSet.add("text2");
        Struct expectedUserTypeData1 = new Struct(userTypeSchema)
                .put("asciiField", "foobar1")
                .put("setField", new ArrayList<>(sourceSet));
        Struct expectedUserTypeData2 = new Struct(userTypeSchema)
                .put("asciiField", "foobar2")
                .put("setField", new ArrayList<>(sourceSet));
        List<Struct> expectedList = new ArrayList<>();
        expectedList.add(expectedUserTypeData1);
        expectedList.add(expectedUserTypeData2);

        Map<String, Object> jsonObject1 = new HashMap<>(2);
        jsonObject1.put("\"asciiField\"", "foobar1");
        jsonObject1.put("\"setField\"", new ArrayList<>(sourceSet));
        Term userTypeObject1 = userType.fromJSONObject(jsonObject1);
        ByteBuffer buffer1 = userTypeObject1.bindAndGet(QueryOptions.DEFAULT);
        ByteBuffer serializedUserTypeObject1 = userType.decompose(buffer1);
        Map<String, Object> jsonObject2 = new HashMap<>(2);
        jsonObject2.put("\"asciiField\"", "foobar2");
        jsonObject2.put("\"setField\"", new ArrayList<>(sourceSet));
        Term userTypeObject2 = userType.fromJSONObject(jsonObject2);
        ByteBuffer buffer2 = userTypeObject2.bindAndGet(QueryOptions.DEFAULT);
        ByteBuffer serializedUserTypeObject2 = userType.decompose(buffer2);
        List<ByteBuffer> originalList = new ArrayList<>();
        originalList.add(serializedUserTypeObject1);
        originalList.add(serializedUserTypeObject2);

        ListType<ByteBuffer> frozenListType = ListType.getInstance(userType, false);
        ByteBuffer serializedList = frozenListType.decompose(originalList);
        Object deserializedList = CassandraTypeDeserializer.deserialize(frozenListType, serializedList);
        assertEquals(expectedList, deserializedList);

        DefaultUserDefinedType userDefinedType = new DefaultUserDefinedType(CqlIdentifier.fromCql("\"barspace\""),
                CqlIdentifier.fromCql("\"FooType\""), true,
                Arrays.asList(CqlIdentifier.fromCql("\"asciiField\""), CqlIdentifier.fromCql("\"setField\"")),
                Arrays.asList(DataTypes.ASCII, DataTypes.frozenSetOf(DataTypes.ASCII)));
        deserializedList = CassandraTypeDeserializer.deserialize(DataTypes.frozenListOf(userDefinedType), serializedList);
        assertEquals(expectedList, deserializedList);
    }

}
