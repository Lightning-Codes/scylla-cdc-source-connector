package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scylladb.cdc.cql.driver3.Driver3Field;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.ChangeSchema.ColumnDefinition;
import com.scylladb.cdc.model.worker.ChangeSchema.ColumnKind;
import com.scylladb.cdc.model.worker.ChangeSchema.CqlType;
import com.scylladb.cdc.model.worker.ChangeSchema.DataType;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.Field;
import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Blob values must reach Kafka as exactly their own bytes.
 *
 * <p>The Scylla Java driver deserializes a blob nested in a collection, tuple or UDT as a {@code
 * duplicate()} view into the whole serialized cell, and Kafka Connect's JsonConverter encodes a
 * ByteBuffer with {@code array()}, which ignores position and limit. Without a copy, each blob
 * element of a {@code map<text, blob>} is emitted as the entire serialized map cell.
 *
 * <p>These tests build values the way production does: serialize with the driver's codecs, then
 * deserialize, so element buffers are real views into a larger array. They then wrap the values as
 * {@link Driver3Field}s the way CDC Java's Driver3ToLibraryTranslator does, run them through the
 * emitter, encode with JsonConverter (schemas disabled, as deployed) and decode the base64. All
 * values are synthetic.
 */
public class BlobSliceBytesTest {

  private static final ProtocolVersion V4 = ProtocolVersion.V4;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final DataType TEXT = new DataType(CqlType.TEXT);
  private static final DataType BLOB = new DataType(CqlType.BLOB);
  private static final DataType TIMEUUID = new DataType(CqlType.TIMEUUID);
  private static final DataType MAP_TEXT_BLOB =
      new DataType(CqlType.MAP, Arrays.asList(TEXT, BLOB), false);
  private static final DataType LIST_BLOB =
      new DataType(CqlType.LIST, Collections.singletonList(BLOB), true);
  private static final DataType NON_FROZEN_LIST_BLOB =
      new DataType(CqlType.LIST, Collections.singletonList(BLOB), false);
  // The CDC log column of a non-frozen list<V> is frozen<map<timeuuid, V>>; Driver3SchemaFactory
  // takes the LIST base type from the base table.
  private static final DataType MAP_TIMEUUID_BLOB =
      new DataType(CqlType.MAP, Arrays.asList(TIMEUUID, BLOB), true);
  private static final DataType SET_BLOB =
      new DataType(CqlType.SET, Collections.singletonList(BLOB), true);
  private static final DataType TUPLE_TEXT_BLOB =
      new DataType(CqlType.TUPLE, Arrays.asList(TEXT, BLOB), true);
  private static final DataType UDT_TEXT_BLOB = udtType();

  private static final String ATTRS = ":attrs";

  /** CDC log columns of an Alternator-like table plus one column per nested blob shape. */
  private static final ChangeSchema CHANGE_SCHEMA =
      new ChangeSchema(
          Arrays.asList(
              column("pk", 0, TEXT, ColumnKind.PARTITION_KEY),
              column("sk", 1, TEXT, ColumnKind.CLUSTERING_KEY),
              column(ATTRS, 2, MAP_TEXT_BLOB, ColumnKind.REGULAR),
              column("bin", 3, BLOB, ColumnKind.REGULAR),
              column("blob_list", 4, LIST_BLOB, ColumnKind.REGULAR),
              column("blob_set", 5, SET_BLOB, ColumnKind.REGULAR),
              column("pair", 6, TUPLE_TEXT_BLOB, ColumnKind.REGULAR),
              column("rec", 7, UDT_TEXT_BLOB, ColumnKind.REGULAR),
              // The base table type is what makes the emitter treat the timeuuid map as a list.
              new ColumnDefinition(
                  "blob_list_nf", 8, MAP_TIMEUUID_BLOB, NON_FROZEN_LIST_BLOB, ColumnKind.REGULAR)));

  private static final ChangeSchema LEGACY_CHANGE_SCHEMA =
      new ChangeSchema(
          Arrays.asList(
              column("pk", 0, TEXT, ColumnKind.PARTITION_KEY),
              column("sk", 1, TEXT, ColumnKind.CLUSTERING_KEY),
              column("bin", 2, BLOB, ColumnKind.REGULAR)));

  private static final CollectionId COLLECTION_ID = new CollectionId(new TableName("ks", "tbl"));

  private static final Schema SOURCE_SCHEMA =
      SchemaBuilder.struct().name("test.Source").optional().build();

  // ---------------------------------------------------------------------------------------------
  // translateFieldToKafka: every nested blob shape
  // ---------------------------------------------------------------------------------------------

  @Test
  void mapOfThreeBlobs_eachEntryEncodesOnlyItsOwnBytes() throws Exception {
    Map<String, byte[]> input = new LinkedHashMap<>();
    input.put("attr_a", tagS("MARK_A_1"));
    input.put("attr_b", tagS("MARK_B_123"));
    input.put("bytes", tagB(0xff, 0x80, 0x00, 0x7f));

    Map<String, ByteBuffer> views = driverMap(input);
    assertViewsIntoLargerArray(views.values());
    Map<ByteBuffer, int[]> before = positions(views.values());

    JsonNode json = encodeColumn(ATTRS, mapField(views));

    assertEquals(input.keySet(), new LinkedHashSet<>(keysOf(json)));
    assertMapEntries(input, json);
    assertPositionsUnchanged(before);
  }

  @Test
  void mapOfOneBlob_entryEncodesOnlyItsOwnBytes() throws Exception {
    Map<String, byte[]> input = Collections.singletonMap("attr_b", tagS("MARK_B_123"));

    Map<String, ByteBuffer> views = driverMap(input);
    // Even a single entry is a view: the cell also holds the entry count, name and lengths.
    assertViewsIntoLargerArray(views.values());
    Map<ByteBuffer, int[]> before = positions(views.values());

    assertMapEntries(input, encodeColumn(ATTRS, mapField(views)));
    assertPositionsUnchanged(before);
  }

  @Test
  void emptyBlob_encodesAsZeroBytes() throws Exception {
    Map<String, byte[]> input = new LinkedHashMap<>();
    input.put("empty", new byte[0]);
    input.put("attr_b", tagS("MARK_B_123"));

    Map<String, ByteBuffer> views = driverMap(input);
    assertEquals(0, views.get("empty").remaining());
    Map<ByteBuffer, int[]> before = positions(views.values());

    JsonNode json = encodeColumn(ATTRS, mapField(views));

    assertMapEntries(input, json);
    assertEquals("", entryValue(json, "empty").asText());
    assertPositionsUnchanged(before);
  }

  @Test
  void listOfBlobs_eachElementEncodesOnlyItsOwnBytes() throws Exception {
    List<byte[]> input = Arrays.asList(tagS("MARK_LIST_A"), tagS("MARK_B_123"), tagB(0x90));
    List<ByteBuffer> in = new ArrayList<>();
    input.forEach(b -> in.add(ByteBuffer.wrap(b)));
    TypeCodec<List<ByteBuffer>> codec = TypeCodec.list(TypeCodec.blob());
    List<ByteBuffer> views = codec.deserialize(codec.serialize(in, V4), V4);
    assertViewsIntoLargerArray(views);
    Map<ByteBuffer, int[]> before = positions(views);

    List<Field> elements = new ArrayList<>();
    views.forEach(v -> elements.add(new Driver3Field(BLOB, v)));
    JsonNode json = encodeColumn("blob_list", new Driver3Field(LIST_BLOB, elements));

    assertEquals(input.size(), json.size());
    for (int i = 0; i < input.size(); i++) {
      assertArrayEquals(input.get(i), decode(json.get(i)), "list element " + i);
    }
    assertPositionsUnchanged(before);
  }

  @Test
  void nonFrozenListOfBlobs_eachElementEncodesOnlyItsOwnBytes() throws Exception {
    // The emitter turns the timeuuid map into an array of its values in key order, so each element
    // is a map value: a view into the whole serialized map cell.
    List<byte[]> input = Arrays.asList(tagS("MARK_NFLIST_A"), tagS("MARK_B_123"), tagB(0x91));
    Map<UUID, ByteBuffer> in = new LinkedHashMap<>();
    for (int i = 0; i < input.size(); i++) {
      // Fixed version-1 UUIDs in ascending time order; UUIDs.startOf would
      // load the driver's UUIDs class, whose static initializer resolves the
      // local host name (seconds on some machines).
      in.put(
          UUID.fromString(String.format("%08x-0000-1000-8000-000000000000", i + 1)),
          ByteBuffer.wrap(input.get(i)));
    }
    TypeCodec<Map<UUID, ByteBuffer>> codec = TypeCodec.map(TypeCodec.timeUUID(), TypeCodec.blob());
    Map<UUID, ByteBuffer> views = codec.deserialize(codec.serialize(in, V4), V4);
    assertViewsIntoLargerArray(views.values());
    Map<ByteBuffer, int[]> before = positions(views.values());

    // Guards the fixture: the column must take the non-frozen list branch, whose schema is an
    // array of BYTES rather than an array of {key, value} entries.
    Schema schema = cellSchema("blob_list_nf");
    assertEquals(Schema.Type.ARRAY, schema.type());
    assertEquals(Schema.Type.BYTES, schema.valueSchema().type());

    Map<Field, Field> entries = new LinkedHashMap<>();
    views.forEach((k, v) -> entries.put(new Driver3Field(TIMEUUID, k), new Driver3Field(BLOB, v)));
    JsonNode json = encode(schema, translate(new Driver3Field(MAP_TIMEUUID_BLOB, entries), schema));

    assertTrue(json.isArray(), "non-frozen list is emitted as an array of values");
    assertEquals(input.size(), json.size());
    for (int i = 0; i < input.size(); i++) {
      assertArrayEquals(input.get(i), decode(json.get(i)), "list element " + i);
    }
    assertPositionsUnchanged(before);
  }

  @Test
  void setOfBlobs_eachElementEncodesOnlyItsOwnBytes() throws Exception {
    List<byte[]> input = Arrays.asList(tagS("MARK_SET_A"), tagS("MARK_SET_B"));
    Set<ByteBuffer> in = new LinkedHashSet<>();
    input.forEach(b -> in.add(ByteBuffer.wrap(b)));
    TypeCodec<Set<ByteBuffer>> codec = TypeCodec.set(TypeCodec.blob());
    Set<ByteBuffer> views = codec.deserialize(codec.serialize(in, V4), V4);
    assertViewsIntoLargerArray(views);
    Map<ByteBuffer, int[]> before = positions(views);

    Set<Field> elements = new LinkedHashSet<>();
    views.forEach(v -> elements.add(new Driver3Field(BLOB, v)));
    JsonNode json = encodeColumn("blob_set", new Driver3Field(SET_BLOB, elements));

    assertEquals(input.size(), json.size());
    for (int i = 0; i < input.size(); i++) {
      assertArrayEquals(input.get(i), decode(json.get(i)), "set element " + i);
    }
    assertPositionsUnchanged(before);
  }

  @Test
  void tupleWithBlob_blobComponentEncodesOnlyItsOwnBytes() throws Exception {
    byte[] payload = tagS("MARK_B_123");
    ByteBuffer view = driverTupleBlob("MARK_TUPLE_TEXT", payload);
    assertViewsIntoLargerArray(Collections.singletonList(view));
    Map<ByteBuffer, int[]> before = positions(Collections.singletonList(view));

    Field tuple =
        new Driver3Field(
            TUPLE_TEXT_BLOB,
            Arrays.<Field>asList(
                new Driver3Field(TEXT, "MARK_TUPLE_TEXT"), new Driver3Field(BLOB, view)));
    JsonNode json = encodeColumn("pair", tuple);

    assertEquals("MARK_TUPLE_TEXT", json.get("field_0").asText());
    assertArrayEquals(payload, decode(json.get("field_1")));
    assertPositionsUnchanged(before);
  }

  @Test
  void udtWithBlob_blobFieldEncodesOnlyItsOwnBytes() throws Exception {
    // Driver 3 has no public UserType constructor. A UDT value has the same wire layout as a
    // tuple of its fields, so the tuple codec yields the same kind of view into the whole value.
    byte[] payload = tagB(0xc3, 0x28, 0xa0);
    ByteBuffer view = driverTupleBlob("MARK_UDT_NAME", payload);
    assertViewsIntoLargerArray(Collections.singletonList(view));
    Map<ByteBuffer, int[]> before = positions(Collections.singletonList(view));

    Map<String, Field> fields = new LinkedHashMap<>();
    fields.put("name", new Driver3Field(TEXT, "MARK_UDT_NAME"));
    fields.put("payload", new Driver3Field(BLOB, view));
    JsonNode json = encodeColumn("rec", new Driver3Field(UDT_TEXT_BLOB, fields));

    assertEquals("MARK_UDT_NAME", json.get("name").asText());
    assertArrayEquals(payload, decode(json.get("payload")));
    assertPositionsUnchanged(before);
  }

  @Test
  void topLevelBlobViews_encodeOnlyTheirOwnBytes() throws Exception {
    // Driver 3 copies each top-level column value into its own array, so production top-level
    // blobs were exact. The conversion must still hold for any view a reader hands over.
    byte[] frame = new byte[32];
    for (int i = 0; i < frame.length; i++) {
      frame[i] = (byte) (0xa0 + i);
    }
    byte[] expected = Arrays.copyOfRange(frame, 7, 12);

    ByteBuffer positioned = ByteBuffer.wrap(frame, 7, 5);
    ByteBuffer offsetSlice = ((ByteBuffer) ByteBuffer.wrap(frame).position(7)).slice();
    offsetSlice.limit(5);
    ByteBuffer readOnly = ByteBuffer.wrap(frame, 7, 5).asReadOnlyBuffer();
    ByteBuffer direct = ByteBuffer.allocateDirect(5).put(expected);
    direct.flip();

    List<ByteBuffer> views = Arrays.asList(positioned, offsetSlice, readOnly, direct);
    Map<ByteBuffer, int[]> before = positions(views);
    for (ByteBuffer view : views) {
      Object translated = translate(new Driver3Field(BLOB, view), cellSchema("bin"));
      assertInstanceOf(byte[].class, translated);
      assertArrayEquals(expected, (byte[]) translated);
      assertArrayEquals(expected, decode(encode(cellSchema("bin"), translated)));
    }
    assertPositionsUnchanged(before);
  }

  // ---------------------------------------------------------------------------------------------
  // Whole records: preimage and postimage through the advanced and legacy emitters
  // ---------------------------------------------------------------------------------------------

  @Test
  void updateRecord_beforeAndAfterEntriesEncodeOnlyTheirOwnBytes() throws Exception {
    Map<String, byte[]> preAttrs = new LinkedHashMap<>();
    preAttrs.put("attr_a", tagS("MARK_A_OLD"));
    preAttrs.put("attr_b", tagS("MARK_B_OLD"));
    Map<String, byte[]> postAttrs = new LinkedHashMap<>();
    postAttrs.put("attr_a", tagS("MARK_A_NEW"));
    postAttrs.put("attr_b", tagS("MARK_B_123"));
    postAttrs.put("count", tagN(0, new byte[] {0x2a}));
    byte[] preBin = tagB(0x81, 0x01);
    byte[] postBin = tagB(0x82, 0x02);
    byte[] pairBlob = tagS("MARK_PAIR");

    RawChange preImage =
        image(RawChange.OperationType.PRE_IMAGE, preAttrs, viewOf(preBin), pairBlob);
    RawChange postImage =
        image(RawChange.OperationType.POST_IMAGE, postAttrs, viewOf(postBin), pairBlob);
    Map<String, Object> delta = keyValues();
    delta.put(ATTRS, mapField(driverMap(postAttrs)).getAsObject());
    delta.put("bin", viewOf(postBin));
    RawChange change = new FixedChange(RawChange.OperationType.ROW_UPDATE, CHANGE_SCHEMA, delta);

    TaskInfo taskInfo =
        new TaskInfo.BeforeAfter().setChange(change).setPreImage(preImage).setPostImage(postImage);
    JsonNode envelope = emitAdvanced(taskInfo);

    assertEquals("u", envelope.get("op").asText());
    JsonNode before = envelope.get("before");
    JsonNode after = envelope.get("after");
    assertMapEntries(preAttrs, before.get(ATTRS));
    assertMapEntries(postAttrs, after.get(ATTRS));
    assertArrayEquals(preBin, decode(before.get("bin")));
    assertArrayEquals(postBin, decode(after.get("bin")));
    // pair is unchanged, so 'before' takes it from the postimage.
    assertArrayEquals(pairBlob, decode(before.get("pair").get("field_1")));
    assertArrayEquals(pairBlob, decode(after.get("pair").get("field_1")));
    assertEquals("MARK_PK", after.get("pk").asText());
    assertEquals("MARK_SK", before.get("sk").asText());
  }

  @Test
  void deleteRecord_preimageEntriesEncodeOnlyTheirOwnBytes() throws Exception {
    Map<String, byte[]> preAttrs = new LinkedHashMap<>();
    preAttrs.put("attr_a", tagS("MARK_A_OLD"));
    preAttrs.put("attr_b", tagS("MARK_B_123"));
    byte[] preBin = tagB(0x83);

    RawChange preImage =
        image(RawChange.OperationType.PRE_IMAGE, preAttrs, viewOf(preBin), tagS("MARK_PAIR"));
    RawChange change =
        new FixedChange(RawChange.OperationType.ROW_DELETE, CHANGE_SCHEMA, keyValues());

    TaskInfo taskInfo = new TaskInfo.BeforeAfter().setChange(change).setPreImage(preImage);
    JsonNode envelope = emitAdvanced(taskInfo);

    assertEquals("d", envelope.get("op").asText());
    assertTrue(envelope.get("after").isNull());
    assertMapEntries(preAttrs, envelope.get("before").get(ATTRS));
    assertArrayEquals(preBin, decode(envelope.get("before").get("bin")));
  }

  @Test
  void legacyUpdateRecord_topLevelBlobEncodesOnlyItsOwnBytes() throws Exception {
    byte[] preBin = tagS("MARK_LEGACY_OLD");
    byte[] postBin = tagS("MARK_B_123");

    Map<String, Object> pre = keyValues();
    pre.put("bin", viewOf(preBin));
    Map<String, Object> post = keyValues();
    post.put("bin", viewOf(postBin));
    RawChange preImage =
        new FixedChange(RawChange.OperationType.PRE_IMAGE, LEGACY_CHANGE_SCHEMA, pre);
    RawChange change =
        new FixedChange(RawChange.OperationType.ROW_UPDATE, LEGACY_CHANGE_SCHEMA, post);

    ScyllaConnectorConfig config = config("legacy");
    ScyllaSchemaLegacy schema = new ScyllaSchemaLegacy(config, SOURCE_SCHEMA);
    ScyllaCollectionSchema collectionSchema =
        schema.updateChangeSchema(COLLECTION_ID, LEGACY_CHANGE_SCHEMA);
    ScyllaChangeRecordEmitterLegacy emitter =
        new ScyllaChangeRecordEmitterLegacy(
            null,
            preImage,
            change,
            Mockito.mock(OffsetContext.class),
            schema,
            Clock.system(),
            config);
    List<Struct> records = new ArrayList<>();
    emitter.emitChangeRecords(
        collectionSchema,
        (partition, dataCollectionSchema, operation, key, value, offset, headers) ->
            records.add(value));

    assertEquals(1, records.size());
    JsonNode envelope = encode(records.get(0).schema(), records.get(0));
    assertArrayEquals(preBin, decode(envelope.get("before").get("bin").get("value")));
    assertArrayEquals(postBin, decode(envelope.get("after").get("bin").get("value")));
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static ColumnDefinition column(String name, int index, DataType type, ColumnKind kind) {
    return new ColumnDefinition(name, index, type, type, kind);
  }

  private static DataType udtType() {
    Map<String, DataType> fields = new LinkedHashMap<>();
    fields.put("name", TEXT);
    fields.put("payload", BLOB);
    return new DataType(CqlType.UDT, new DataType.UdtType(fields, "ks", "rec_t"), true);
  }

  /** The deployed connector settings for advanced; legacy takes none of the include options. */
  private static ScyllaConnectorConfig config(String outputFormat) {
    Configuration.Builder builder =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
            .with("scylla.table.names", "ks.tbl")
            .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, outputFormat);
    if ("advanced".equals(outputFormat)) {
      builder =
          builder
              .with(ScyllaConnectorConfig.CDC_INCLUDE_BEFORE_KEY, "full")
              .with(ScyllaConnectorConfig.CDC_INCLUDE_AFTER_KEY, "full")
              .with(
                  ScyllaConnectorConfig.CDC_INCLUDE_PK_KEY,
                  "kafka-key,payload-after,payload-before");
    }
    return new ScyllaConnectorConfig(builder.build());
  }

  private static ScyllaCollectionSchema collectionSchema() {
    return new ScyllaSchema(config("advanced"), SOURCE_SCHEMA)
        .updateChangeSchema(COLLECTION_ID, CHANGE_SCHEMA);
  }

  private static Schema cellSchema(String column) {
    Schema schema = collectionSchema().cellSchema(column);
    assertNotNull(schema, column);
    return schema;
  }

  /** Calls the emitter's private translateFieldToKafka, the single path for every image value. */
  private static Object translate(Field field, Schema schema) throws Exception {
    Method method =
        ScyllaChangeRecordEmitter.class.getDeclaredMethod(
            "translateFieldToKafka", Field.class, Schema.class);
    method.setAccessible(true);
    ScyllaChangeRecordEmitter emitter =
        new ScyllaChangeRecordEmitter(null, null, null, null, null, config("advanced"));
    try {
      return method.invoke(emitter, field, schema);
    } catch (InvocationTargetException e) {
      throw (Exception) e.getCause();
    }
  }

  private static JsonNode encodeColumn(String column, Field field) throws Exception {
    Schema schema = cellSchema(column);
    return encode(schema, translate(field, schema));
  }

  /** Encodes like the deployed worker: JsonConverter with schemas.enable=false. */
  private static JsonNode encode(Schema schema, Object value) throws Exception {
    JsonConverter converter = new JsonConverter();
    converter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    return MAPPER.readTree(converter.fromConnectData("topic", schema, value));
  }

  private static JsonNode emitAdvanced(TaskInfo taskInfo) throws Exception {
    ScyllaConnectorConfig config = config("advanced");
    ScyllaSchema schema = new ScyllaSchema(config, SOURCE_SCHEMA);
    ScyllaCollectionSchema collectionSchema =
        schema.updateChangeSchema(COLLECTION_ID, CHANGE_SCHEMA);
    ScyllaChangeRecordEmitter emitter =
        new ScyllaChangeRecordEmitter(
            null, taskInfo, Mockito.mock(OffsetContext.class), schema, Clock.system(), config);
    List<Struct> records = new ArrayList<>();
    emitter.emitChangeRecords(
        collectionSchema,
        (partition, dataCollectionSchema, operation, key, value, offset, headers) ->
            records.add(value));
    assertEquals(1, records.size());
    return encode(records.get(0).schema(), records.get(0));
  }

  /** A pre- or postimage row holding every nested blob shape, built from driver views. */
  private static RawChange image(
      RawChange.OperationType type, Map<String, byte[]> attrs, ByteBuffer bin, byte[] pairBlob) {
    Map<String, Object> values = keyValues();
    values.put(ATTRS, mapField(driverMap(attrs)).getAsObject());
    values.put("bin", bin);
    values.put(
        "pair",
        Arrays.<Field>asList(
            new Driver3Field(TEXT, "MARK_PAIR_TEXT"),
            new Driver3Field(BLOB, driverTupleBlob("MARK_PAIR_TEXT", pairBlob))));
    return new FixedChange(type, CHANGE_SCHEMA, values);
  }

  private static Map<String, Object> keyValues() {
    Map<String, Object> values = new HashMap<>();
    values.put("pk", "MARK_PK");
    values.put("sk", "MARK_SK");
    return values;
  }

  /** Serializes then deserializes map<text, blob> with the driver, as it reads a CDC cell. */
  private static Map<String, ByteBuffer> driverMap(Map<String, byte[]> entries) {
    Map<String, ByteBuffer> in = new LinkedHashMap<>();
    entries.forEach((k, v) -> in.put(k, ByteBuffer.wrap(v)));
    TypeCodec<Map<String, ByteBuffer>> codec = TypeCodec.map(TypeCodec.varchar(), TypeCodec.blob());
    return codec.deserialize(codec.serialize(in, V4), V4);
  }

  /** Returns the blob component of a driver-deserialized tuple<text, blob>. */
  private static ByteBuffer driverTupleBlob(String text, byte[] blob) {
    TupleType type =
        TupleType.of(
            V4,
            CodecRegistry.DEFAULT_INSTANCE,
            com.datastax.driver.core.DataType.text(),
            com.datastax.driver.core.DataType.blob());
    TypeCodec<TupleValue> codec = TypeCodec.tuple(type);
    TupleValue in = type.newValue(text, ByteBuffer.wrap(blob));
    TupleValue out = codec.deserialize(codec.serialize(in, V4), V4);
    return (ByteBuffer) out.getObject(1);
  }

  /** Wraps a driver map the way Driver3ToLibraryTranslator does. */
  private static Field mapField(Map<String, ByteBuffer> driverMap) {
    Map<Field, Field> translated = new LinkedHashMap<>();
    driverMap.forEach(
        (k, v) -> translated.put(new Driver3Field(TEXT, k), new Driver3Field(BLOB, v)));
    return new Driver3Field(MAP_TEXT_BLOB, translated);
  }

  /** A worst-case top-level view: the blob sits in the middle of a larger array. */
  private static ByteBuffer viewOf(byte[] blob) {
    byte[] frame = new byte[blob.length + 9];
    Arrays.fill(frame, (byte) 0x5a);
    System.arraycopy(blob, 0, frame, 4, blob.length);
    return ByteBuffer.wrap(frame, 4, blob.length);
  }

  /** Guards the fixture: the buffers must look like production's, not exact arrays. */
  private static void assertViewsIntoLargerArray(Iterable<ByteBuffer> views) {
    for (ByteBuffer view : views) {
      assertTrue(view.hasArray());
      assertTrue(
          view.array().length > view.remaining(),
          "expected a view into a larger array, got an exact buffer");
    }
  }

  private static Map<ByteBuffer, int[]> positions(Iterable<ByteBuffer> views) {
    // Identity, not equals: ByteBuffer.equals compares contents, and two views may be equal.
    Map<ByteBuffer, int[]> positions = new IdentityHashMap<>();
    for (ByteBuffer view : views) {
      positions.put(view, new int[] {view.position(), view.limit()});
    }
    return positions;
  }

  private static void assertPositionsUnchanged(Map<ByteBuffer, int[]> before) {
    before.forEach(
        (view, positionAndLimit) -> {
          assertEquals(positionAndLimit[0], view.position(), "position changed");
          assertEquals(positionAndLimit[1], view.limit(), "limit changed");
        });
  }

  private static void assertMapEntries(Map<String, byte[]> expected, JsonNode entries) {
    assertNotNull(entries);
    assertTrue(entries.isArray(), "map is emitted as an array of {key, value}");
    assertEquals(expected.size(), entries.size());
    expected.forEach(
        (name, bytes) -> assertArrayEquals(bytes, decode(entryValue(entries, name)), name));
  }

  private static JsonNode entryValue(JsonNode entries, String name) {
    for (JsonNode entry : entries) {
      if (name.equals(entry.get("key").asText())) {
        return entry.get("value");
      }
    }
    throw new AssertionError("no entry " + name);
  }

  private static List<String> keysOf(JsonNode entries) {
    List<String> keys = new ArrayList<>();
    entries.forEach(entry -> keys.add(entry.get("key").asText()));
    return keys;
  }

  private static byte[] decode(JsonNode base64) {
    assertNotNull(base64);
    assertTrue(base64.isTextual(), "blob is emitted as a base64 string: " + base64);
    return Base64.getDecoder().decode(base64.asText());
  }

  /** Alternator S value: tag 0, then UTF-8. */
  private static byte[] tagS(String value) {
    byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    byte[] out = new byte[utf8.length + 1];
    System.arraycopy(utf8, 0, out, 1, utf8.length);
    return out;
  }

  /** Alternator B value: tag 1, then the raw bytes (each int is truncated to a byte). */
  private static byte[] tagB(int... raw) {
    byte[] out = new byte[raw.length + 1];
    out[0] = 1;
    for (int i = 0; i < raw.length; i++) {
      out[i + 1] = (byte) raw[i];
    }
    return out;
  }

  /** Alternator N value: tag 3, int32 BE scale, then the BE two's-complement unscaled value. */
  private static byte[] tagN(int scale, byte[] unscaled) {
    return ByteBuffer.allocate(1 + 4 + unscaled.length)
        .put((byte) 3)
        .putInt(scale)
        .put(unscaled)
        .array();
  }

  /** A CDC row with fixed values, exposed the way Driver3RawChange exposes translated values. */
  private static final class FixedChange implements RawChange {
    private final OperationType operationType;
    private final ChangeSchema schema;
    private final Map<String, Object> values;

    FixedChange(OperationType operationType, ChangeSchema schema, Map<String, Object> values) {
      this.operationType = operationType;
      this.schema = schema;
      this.values = values;
    }

    @Override
    public OperationType getOperationType() {
      return operationType;
    }

    @Override
    public ChangeSchema getSchema() {
      return schema;
    }

    @Override
    public Object getAsObject(ColumnDefinition c) {
      return values.get(c.getColumnName());
    }

    @Override
    public Cell getCell(ColumnDefinition c) {
      return new FixedCell(c, values.get(c.getColumnName()));
    }

    @Override
    public boolean isNull(ColumnDefinition c) {
      return values.get(c.getColumnName()) == null;
    }

    @Override
    public ByteBuffer getAsUnsafeBytes(ColumnDefinition c) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class FixedCell implements Cell {
    private final ColumnDefinition columnDefinition;
    private final Object value;

    FixedCell(ColumnDefinition columnDefinition, Object value) {
      this.columnDefinition = columnDefinition;
      this.value = value;
    }

    @Override
    public ColumnDefinition getColumnDefinition() {
      return columnDefinition;
    }

    @Override
    public Object getAsObject() {
      return value;
    }

    @Override
    public DataType getDataType() {
      return columnDefinition.getCdcLogDataType();
    }

    @Override
    public ByteBuffer getAsUnsafeBytes() {
      throw new UnsupportedOperationException();
    }
  }
}
