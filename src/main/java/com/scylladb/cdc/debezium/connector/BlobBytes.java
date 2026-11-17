package com.scylladb.cdc.debezium.connector;

import java.nio.ByteBuffer;

/**
 * Hands CQL blob values to Kafka Connect as byte arrays that hold exactly the blob's bytes.
 *
 * <p>The Scylla Java driver deserializes a blob nested in a collection, tuple or UDT as a {@code
 * duplicate()} view: position and limit select the element, and the backing array is the whole
 * serialized cell. Kafka Connect's JsonConverter encodes a {@link ByteBuffer} with {@code array()},
 * which ignores position, limit and array offset, so each blob element of a collection would be
 * emitted as the entire serialized cell. Converters encode a {@code byte[]} as is, and Connect
 * accepts {@code byte[]} for BYTES schemas. Top-level columns are not affected today: driver 3
 * copies each column value into its own array.
 */
final class BlobBytes {

  private BlobBytes() {}

  /**
   * Returns a copy of the remaining bytes of {@code value} if it is a {@link ByteBuffer}, else
   * {@code value} unchanged.
   *
   * <p>Reads through {@code duplicate()} so the caller's buffer keeps its position. A nested CDC
   * Java {@code Field} (collection element, tuple or UDT field) returns the same buffer on every
   * call, and {@code ByteBuffer.equals} and {@code hashCode} depend on the position, so consuming
   * it would, for example, change the hash of a blob map key while it sits in the translated map.
   * Direct and read-only buffers, whose array JsonConverter cannot read at all, are copied the same
   * way.
   */
  static Object toConnectValue(Object value) {
    if (!(value instanceof ByteBuffer)) {
      return value;
    }
    ByteBuffer view = ((ByteBuffer) value).duplicate();
    byte[] bytes = new byte[view.remaining()];
    view.get(bytes);
    return bytes;
  }
}
