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
package io.aiven.kafka.connect.amqp.sink.strategy;

import io.aiven.kafka.connect.amqp.common.AmqpParseException;
import io.aiven.kafka.connect.amqp.sink.errant.ErrantRecordHandler;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standard AMQP message strategy. This message assumes that:
 *
 * <ul>
 *   <li>The Kafka message value is the same as the AMQP message body.  Different encoding, same data.
 *   <li>The Kafka headers contains entries that start with "amqp" that are to be used to populate the AMQP message properties, footers, and attributes.</li>
 * </ul>
 */
public class AmqpBodyFmt extends AbstractAmqpStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpBodyFmt.class);

  /**
   * Constructs a body format strategy.
   *
   * @param sender the sender to send the AMQP messages with.
   * @param errantRecordHandler the handler for bad records.
   */
  public AmqpBodyFmt(Sender sender, ErrantRecordHandler errantRecordHandler) {
    super(sender, errantRecordHandler);
  }

  @Override
  ClientMessage<?> createClientMessage(SinkRecord sinkRecord)
      throws AmqpParseException, ClientException {
    ClientMessage<?> message = constructMessage(sinkRecord);
    for (Header h : sinkRecord.headers()) {
      parseHeader(message, h);
    }
    return message;
  }

  /**
   * Constructs teh Message and populates the body.
   *
   * @param sinkRecord the sink record to populate the message from.
   * @return populated Message.
   * @throws AmqpParseException on AMQP data issue.
   */
  private ClientMessage<?> constructMessage(SinkRecord sinkRecord) throws AmqpParseException {
    if (sinkRecord.value() == null) {
      return ClientMessage.create();
    }
    if (sinkRecord.valueSchema() != null) {
      return ClientMessage.create(parseBody(sinkRecord.valueSchema(), sinkRecord.value()));
    }
    // body must be string or byte[]
    if (sinkRecord.value() instanceof byte[] bytes) {
      return ClientMessage.create(new Data(bytes));
    }
    if (sinkRecord.value() instanceof String str) {
      return ClientMessage.create(new AmqpValue<>(str));
    }
    throw new AmqpParseException(
        String.format(
            "body value does not have a schema and is not a String or byte[]: %s",
            sinkRecord.value().getClass()));
  }

  /**
   * Transcode the schema and body values from the Kafka sink record into the AMQP body.
   * @param bodySchema the sink record body schema.
   * @param bodyValue the sink record body value.
   * @return A QPID Section that is the AMQP encoding for the body value.
   * @throws AmqpParseException if the transcoding can not be performed.
   */
  private Section<?> parseBody(Schema bodySchema, Object bodyValue) throws AmqpParseException {
    SchemaAndValue schemaAndValue = new SchemaAndValue(bodySchema, bodyValue);
    Optional<Object> optObj = encoderDecoder.decode(schemaAndValue);
    if (optObj.isPresent()) {
      Object object = optObj.get();
      if (object instanceof List<?> lst) {
        if (lst.size() == 1) {
          if (lst.get(0) instanceof byte[] buffer) {
            return new Data(buffer);
          }
          return new AmqpValue<>(lst.get(0));
        }
        if (lst.size() > 1) {
          return new AmqpSequence<>(lst);
        } else {
          throw new AmqpParseException(
              String.format("Empty list extracted from %s with schema %s", bodyValue, bodySchema));
        }
      } else {
        return new AmqpValue<>(object);
      }
    } else {
      throw new AmqpParseException(
          String.format("Unable to extract data from %s with schema %s", bodyValue, bodySchema));
    }
  }

  /**
   * Sets the AMQP message property from the object.
   * @param key the header key.  These are prefixed with "amqp."
   * @param message the message to set the value in.
   * @param value the value to encode into the proper AMQP format.
   * @throws ClientException on AMQP error.
   */
  private void setValue(String key, Message<?> message, Object value) throws ClientException {
    switch (key) {
      case "amqp.messageId" -> {
        if (value instanceof String str) {
          try {
            message.messageId(UUID.fromString(str));
          } catch (IllegalArgumentException expected) {
            message.messageId(value);
          }
        } else {
          message.messageId(value);
        }
      }
      case "amqp.userId" -> {
        if (value instanceof String str) {
          message.userId(Base64.decodeBase64(str));
        } else {
          message.userId((byte[]) value);
        }
      }
      case "amqp.to" -> message.to(value.toString());
      case "amqp.subject" -> message.subject(value.toString());
      case "amqp.replyTo" -> message.replyTo(value.toString());
      case "amqp.correlationId" -> message.correlationId(value);
      case "amqp.contentType" -> message.contentType(value.toString());
      case "amqp.contentEncoding" -> message.contentEncoding(value.toString());
      case "amqp.absoluteExpiry" -> {
        Optional<Number> n = getNumber(key, value);
        if (n.isPresent()) {
          message.absoluteExpiryTime(n.get().longValue());
        }
      }
      case "amqp.creationTime" -> {
        Optional<Number> n = getNumber(key, value);
        if (n.isPresent()) {
          message.creationTime(n.get().longValue());
        }
      }
      case "amqp.groupId" -> message.groupId(value.toString());
      case "amqp.groupSequence" -> {
        Optional<Number> n = getNumber(key, value);
        if (n.isPresent()) {
          message.groupSequence(n.get().intValue());
        }
      }
      case "amqp.replyToGroupId" -> message.replyToGroupId(value.toString());
      case "amqp.durable" -> message.durable(getBoolean(value));
      case "amqp.firstAcquirer" -> message.firstAcquirer(getBoolean(value));
      case "amqp.deliveryCount" -> {
        Optional<Number> n = getNumber(key, value);
        if (n.isPresent()) {
          message.deliveryCount(n.get().longValue());
        }
      }
      case "amqp.footers" -> {
        Map<String, ?> map = (Map<String, ?>) value;

        map.forEach(
            (k, v) -> {
              try {
                message.footer(k, v);
              } catch (ClientException e) {
                LOGGER.error("Unable to write footer {}: {}", key, v);
              }
            });
      }
      case "amqp.annotations" -> {
        Map<String, ?> map = (Map<String, ?>) value;

        map.forEach(
            (k, v) -> {
              try {
                message.annotation(k, v);
              } catch (ClientException e) {
                LOGGER.error("Unable to write annotation {}: {}", key, v);
              }
            });
      }
    }
  }

  /**
   * Converts an object to a number, if it is not already one.
   * @param key the key.  Used for error reporting.
   * @param object the object to convert.
   * @return and Optional number if the number cojuld be converted, an empty optional otherwise.
   */
  private Optional<Number> getNumber(String key, Object object) {
    if (object instanceof Number number) {
      return Optional.of(number);
    }
    LOGGER.error("Key {} is not a Number: {}", key, object.getClass());
    return Optional.empty();
  }

  /**
   * Gets the boolean value of the object.
   * If the object is an instance of Boolean return it, othersie parse the string value of the object as a boolean.
   * @param object the object to convert.
   * @return the boolean value.
   */
  private boolean getBoolean(Object object) {
    return object instanceof Boolean bool ? bool : Boolean.parseBoolean(object.toString());
  }

  /**
   * Parse and Kafka header into the proper value in the AMQP message.
   * Will only process headers whos key starts with "amqp." all others are ignored.
   * @param message the message to populate.
   * @param header the header to convert.
   * @throws ClientException if the message value can not be set.
   */
  private void parseHeader(Message<?> message, Header header) throws ClientException {
    if (header.key().startsWith("amqp.")) {
      Optional<Object> decodeResult =
          encoderDecoder.decode(new SchemaAndValue(header.schema(), header.value()));
      if (decodeResult.isPresent()) {
        setValue(header.key(), message, decodeResult.get());
      }
    }
  }
}
