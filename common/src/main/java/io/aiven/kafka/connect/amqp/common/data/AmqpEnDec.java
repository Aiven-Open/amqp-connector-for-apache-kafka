/*
        Copyright 2026 Aiven Oy and project contributors

       Licensed under the Apache License, Version 2.0 (the "License");
       you may not use this file except in compliance with the License.
       You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License.

       SPDX-License-Identifier: Apache-2.0
*/
package io.aiven.kafka.connect.amqp.common.data;

import io.aiven.kafka.connect.amqp.common.config.AmqpHeaderProperties;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts AMQP values. */
public final class AmqpEnDec extends EncoderDecoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpEnDec.class);

  /** Constructor. */
  public AmqpEnDec() {}

  @Override
  public Optional<SchemaAndValue> encode(Object value) {
    if (value == null) {
      return Optional.empty();
    }
    String name = asName(value.getClass());
    if (name.startsWith("org.apache.qpid.protonj2.types.")) {
      if (value instanceof Number n) {
        if (value instanceof UnsignedByte) {
          return Optional.of(
              new SchemaAndValue(
                  new SchemaBuilder(Schema.Type.INT16).name(name).build(), n.shortValue()));
        }
        if (value instanceof UnsignedShort) {
          return Optional.of(
              new SchemaAndValue(
                  new SchemaBuilder(Schema.Type.INT32).name(name).build(), n.intValue()));
        }
        if (value instanceof UnsignedInteger) {
          return Optional.of(
              new SchemaAndValue(
                  new SchemaBuilder(Schema.Type.INT64).name(name).build(), n.longValue()));
        }
        if (value instanceof UnsignedLong) {
          return Optional.of(
              new SchemaAndValue(
                  new SchemaBuilder(Schema.Type.STRING).name(name).build(), value.toString()));
        }
      }

      if (value instanceof Binary) {
        return Optional.of(
            new SchemaAndValue(
                new SchemaBuilder(Schema.Type.BYTES).name(name).build(),
                ((Binary) value).asByteArray()));
      }

      if (value instanceof Symbol) {
        return Optional.of(
            new SchemaAndValue(
                new SchemaBuilder(Schema.Type.STRING).name(name).optional().build(),
                value.toString()));
      }

      if (value instanceof MessageAnnotations annotations) {
        return symbolObjectMap(name, annotations.getValue());
      }

      if (value instanceof Footer footers) {
        return symbolObjectMap(name, footers.getValue());
      }

      if (value instanceof Properties properties) {
        Map<String, SchemaAndValue> map = new TreeMap<>();
        for (AmqpHeaderProperties property : AmqpHeaderProperties.values()) {
          switch (property) {
            case MESSAGE_ID -> {
              if (properties.hasMessageId()) {
                self()
                    .encode(properties.getMessageId())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case USER_ID -> {
              if (properties.hasUserId()) {
                self()
                    .encode(properties.getUserId())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case TO -> {
              if (properties.hasTo()) {
                self()
                    .encode(properties.getTo())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case SUBJECT -> {
              if (properties.hasSubject()) {
                self()
                    .encode(properties.getSubject())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case REPLY_TO -> {
              if (properties.hasReplyTo()) {
                self()
                    .encode(properties.getReplyTo())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case CORRELATION_ID -> {
              if (properties.hasCorrelationId()) {
                self()
                    .encode(properties.getCorrelationId())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case CONTENT_TYPE -> {
              if (properties.hasContentType()) {
                self()
                    .encode(properties.getContentType())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case CONTENT_ENCODING -> {
              if (properties.hasContentEncoding()) {
                self()
                    .encode(properties.getContentEncoding())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case ABSOLUTE_EXPIRY -> {
              if (properties.hasAbsoluteExpiryTime()) {
                self()
                    .encode(properties.getAbsoluteExpiryTime())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case CREATION_TIME -> {
              if (properties.hasCreationTime()) {
                self()
                    .encode(properties.getCreationTime())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case GROUP_ID -> {
              if (properties.hasGroupId()) {
                self()
                    .encode(properties.getGroupId())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case GROUP_SEQUENCE -> {
              if (properties.hasGroupSequence()) {
                self()
                    .encode(properties.getGroupSequence())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
            case REPLY_TO_GROUP_ID -> {
              if (properties.hasReplyToGroupId()) {
                self()
                    .encode(properties.getReplyToGroupId())
                    .ifPresent(s -> map.put(property.getSchemaName(), s));
              }
            }
          }
        }
        SchemaBuilder schema =
            new SchemaBuilder(Schema.Type.STRUCT).name(Properties.class.getName());
        map.forEach((k, v) -> schema.field(k, v.schema()));

        Struct struct = new Struct(schema.build());
        map.forEach((k, v) -> struct.put(k, v.value()));
        return Optional.of(new SchemaAndValue(struct.schema(), struct));
      }

      if (value instanceof Section section) {
        self().encode(section.getValue());
      }
    }
    return Optional.empty();
  }

  /**
   * Converts a Synmbol/Object map into a struct based SchemaAndValue. Symbol order in the map is
   * retained as field order in the struct.
   *
   * @param name the name of the resulting struct.
   * @param data the map to place into the struct.
   * @return the struct based SchemaAndValue.
   */
  private Optional<SchemaAndValue> symbolObjectMap(String name, Map<Symbol, Object> data) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(name);
    Map<String, Object> values = new HashMap<>();
    data.forEach(
        (key1, value) -> {
          self()
              .encode(value)
              .ifPresentOrElse(
                  schemaAndValue -> {
                    String key = key1.toString();
                    schemaBuilder.field(key, schemaAndValue.schema());
                    values.put(key, schemaAndValue.value());
                  },
                  () ->
                      LOGGER.warn(
                          "unable to encode {} with {}", value.getClass(), self().toString()));
        });
    Schema schema = schemaBuilder.build();
    Struct struct = new Struct(schema);
    for (Field field : schema.fields()) {
      struct.put(field, values.get(field.name()));
    }
    return Optional.of(new SchemaAndValue(schema, struct));
  }

  @Override
  public Optional<Object> decode(SchemaAndValue schemaAndValue) {
    String name = schemaAndValue.schema().name();
    if (name != null && name.startsWith("org.apache.qpid.protonj2.types.")) {
      if (isName(UnsignedByte.class, name)) {
        Number n = (Number) schemaAndValue.value();
        return Optional.of(new UnsignedByte(n.byteValue()));
      }

      if (isName(UnsignedShort.class, name)) {
        Number n = (Number) schemaAndValue.value();
        return Optional.of(new UnsignedShort(n.shortValue()));
      }

      if (isName(UnsignedInteger.class, name)) {
        Number n = (Number) schemaAndValue.value();
        return Optional.of(new UnsignedInteger(n.intValue()));
      }

      if (isName(UnsignedLong.class, name)) {
        return Optional.of(UnsignedLong.valueOf((String) schemaAndValue.value()));
      }

      if (isName(Binary.class, name)) {
        return Optional.of(new Binary((byte[]) schemaAndValue.value()));
      }

      if (isName(Symbol.class, name)) {
        return Optional.ofNullable(Symbol.getSymbol((String) schemaAndValue.value()));
      }

      if (isName(MessageAnnotations.class, name)) {
        return Optional.of(new MessageAnnotations(extractSymbolMap(schemaAndValue)));
      }

      if (isName(Footer.class, name)) {
        return Optional.of(new Footer(extractSymbolMap(schemaAndValue)));
      }

      if (isName(Properties.class, name)) {
        if (schemaAndValue.value() instanceof Struct struct) {
          Properties result = new Properties();
          for (AmqpHeaderProperties property : AmqpHeaderProperties.values()) {
            Object value = struct.get(property.getSchemaName());
            if (value != null) {
              switch (property) {
                case MESSAGE_ID -> {
                  result.setMessageId(value);
                }
                case USER_ID -> {
                  if (value instanceof byte[] bytes) {
                    result.setUserId(bytes);
                  } else if (value instanceof Binary binary) {
                    result.setUserId(binary);
                  } else {
                    LOGGER.warn("unable to parse userId from {}", value.getClass());
                  }
                }
                case TO -> {
                  result.setTo(value.toString());
                }
                case SUBJECT -> {
                  result.setSubject(value.toString());
                }
                case REPLY_TO -> {
                  result.setReplyTo(value.toString());
                }
                case CORRELATION_ID -> {
                  result.setCorrelationId(value);
                }
                case CONTENT_TYPE -> {
                  result.setContentType(value.toString());
                }
                case CONTENT_ENCODING -> {
                  result.setContentEncoding(value.toString());
                }
                case ABSOLUTE_EXPIRY -> {
                  if (value instanceof Number number) {
                    result.setAbsoluteExpiryTime(number.longValue());
                  } else {
                    LOGGER.warn("unable to parse absoluteExpiryTime from {}", value.getClass());
                  }
                }
                case CREATION_TIME -> {
                  if (value instanceof Number number) {
                    result.setCreationTime(number.longValue());
                  } else {
                    LOGGER.warn("unable to parse creationTime from {}", value.getClass());
                  }
                }
                case GROUP_ID -> {
                  result.setGroupId(value.toString());
                }
                case GROUP_SEQUENCE -> {
                  if (value instanceof Number number) {
                    result.setGroupSequence(number.longValue());
                  } else {
                    LOGGER.warn("unable to parse groupSequence from {}", value.getClass());
                  }
                }
                case REPLY_TO_GROUP_ID -> {
                  result.setReplyToGroupId(value.toString());
                }
              }
            }
          }
          return Optional.of(result);
        } else {
          LOGGER.warn("unable to parse {} as Properties", schemaAndValue);
        }
      }
    }
    return Optional.empty();
  }

  private Map<Symbol, Object> extractSymbolMap(final SchemaAndValue schemaAndValue) {
    Map<Symbol, Object> symbolMap = new LinkedHashMap<>();
    Struct values = (Struct) schemaAndValue.value();

    for (Field field : schemaAndValue.schema().fields()) {
      Symbol fieldSymbol = Symbol.valueOf(field.name());
      SchemaAndValue fieldValue = new SchemaAndValue(field.schema(), values.get(field));
      self()
          .decode(fieldValue)
          .ifPresentOrElse(
              object -> symbolMap.put(fieldSymbol, object),
              () -> symbolMap.put(fieldSymbol, fieldValue.value()));
    }
    return symbolMap;
  }
}
