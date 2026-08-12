package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scylladb.cdc.model.TableName;
import io.debezium.config.Configuration;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

class ConfigSerializerUtilQuotedIdentifierTest {
  @Test
  void deserializesAlternatorTableWithHyphens() {
    assertEquals(
        Set.of(new TableName("alternator_sophena-main-prod", "sophena-main-prod")),
        ConfigSerializerUtil.deserializeTableNames(
            "\"alternator_sophena-main-prod\".\"sophena-main-prod\""));
  }

  @Test
  void keepsDelimitersAndEscapedQuotesInsideQuotedIdentifiers() {
    assertEquals(
        Set.of(
            new TableName("keyspace.with.dot", "table,with,commas"),
            new TableName("keyspace", "table\"with-quote")),
        ConfigSerializerUtil.deserializeTableNames(
            "\"keyspace.with.dot\".\"table,with,commas\", "
                + "\"keyspace\".\"table\"\"with-quote\""));
  }

  @Test
  void validatesQuotedAlternatorTableName() {
    ConfigValue tableNames = validate("\"alternator_sophena-main-prod\".\"sophena-main-prod\"");

    assertTrue(tableNames.errorMessages().isEmpty());
  }

  @Test
  void rejectsMalformedQuotedTableNames() {
    assertFalse(validate("\"keyspace\".\"unterminated").errorMessages().isEmpty());
    assertFalse(validate("\"\".\"table\"").errorMessages().isEmpty());
    assertFalse(validate("\"keyspace\".\"table\".extra").errorMessages().isEmpty());
    assertFalse(validate("\"keyspace\".\"unsafe;task\"").errorMessages().isEmpty());
    assertFalse(
        validate("\"keyspace\".\"identifier-longer-than-forty-eight-utf8-bytes-is-rejected\"")
            .errorMessages()
            .isEmpty());
  }

  private static ConfigValue validate(String serializedTableNames) {
    Configuration config =
        Configuration.create()
            .with("name", "quoted-identifiers-test")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
            .with(ScyllaConnectorConfig.TABLE_NAMES, serializedTableNames)
            .build();
    Map<String, ConfigValue> validation = config.validate(ScyllaConnectorConfig.ALL_FIELDS);
    return validation.get(ScyllaConnectorConfig.TABLE_NAMES.name());
  }
}
