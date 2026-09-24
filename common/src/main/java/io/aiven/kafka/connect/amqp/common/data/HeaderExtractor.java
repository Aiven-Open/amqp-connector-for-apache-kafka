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
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Headers;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Extracts the Kafka connect headers from the AMQP message. */
public class HeaderExtractor {
  /** The data converter to use */
  private final EncoderDecoder dataConverter;

  /** Logger for messages */
  private static final Logger LOGGER = LoggerFactory.getLogger(HeaderExtractor.class);

  /**
   * Create the header extractor with the specified data converter.
   *
   * @param converter the Data converter to use.
   */
  public HeaderExtractor(EncoderDecoder converter) {
    this.dataConverter = converter;
  }

  /**
   * Write the value into a header field. Uses the {@link #dataConverter} to convert the value into
   * a {@link SchemaAndValue} the result is placed in the header named "amqp.{@code <name>}".
   *
   * @param headers the headers to update.
   * @param name the name for the value.
   * @param value the value to write.
   */
  private void writeObject(Headers headers, String name, Object value) {
    if (value != null) {
      dataConverter
          .encode(value)
          .ifPresentOrElse(
              schemaAndValue -> headers.add("amqp." + name, schemaAndValue),
              () -> LOGGER.warn("Unknown data type {} for {}", value.getClass(), name));
    }
  }

  /**
   * processes the message and writes all non-body AMQP values into Kafka connect headers. the
   * {@link AmqpHeaderProperties} enumeration specifies the AMQP message methods that will be
   * extracted and placed into the specified headers. See {@link #writeObject} for discussion of
   * value encoding and header naming.
   *
   * <p>In addition to the listed properties, all annotations and footers will be encoded as map
   * objects in the "amqp.annotations" and "amqp.footers" headers respectively.
   *
   * @param headers the Kafka connect headers to update.
   * @param message the AMQP message to parse.
   * @return the {@code headers} parameter after all updates have been completed.
   * @throws ClientException on AMQP message extraction error
   */
  public Headers processHeaders(Headers headers, Message<?> message) throws ClientException {
    for (AmqpHeaderProperties property : AmqpHeaderProperties.values()) {
      switch (property) {
        case MESSAGE_ID -> writeObject(headers, property.getSchemaName(), message.messageId());
        case USER_ID -> writeObject(headers, property.getSchemaName(), message.userId());
        case TO -> writeObject(headers, property.getSchemaName(), message.to());
        case SUBJECT -> writeObject(headers, property.getSchemaName(), message.subject());
        case REPLY_TO -> writeObject(headers, property.getSchemaName(), message.replyTo());
        case CORRELATION_ID ->
            writeObject(headers, property.getSchemaName(), message.correlationId());
        case CONTENT_TYPE -> writeObject(headers, property.getSchemaName(), message.contentType());
        case CONTENT_ENCODING ->
            writeObject(headers, property.getSchemaName(), message.contentEncoding());
        case ABSOLUTE_EXPIRY ->
            writeObject(headers, property.getSchemaName(), message.absoluteExpiryTime());
        case CREATION_TIME ->
            writeObject(headers, property.getSchemaName(), message.creationTime());
        case GROUP_ID -> writeObject(headers, property.getSchemaName(), message.groupId());
        case GROUP_SEQUENCE ->
            writeObject(headers, property.getSchemaName(), message.groupSequence());
        case REPLY_TO_GROUP_ID ->
            writeObject(headers, property.getSchemaName(), message.replyToGroupId());
        case DURABLE -> writeObject(headers, property.getSchemaName(), message.durable());
        case FIRST_ACQUIRER ->
            writeObject(headers, property.getSchemaName(), message.firstAcquirer());
        case DELIVERY_COUNT ->
            writeObject(headers, property.getSchemaName(), message.deliveryCount());
      }
    }

    if (message.hasAnnotations()) {
      writeObject(headers, "annotations", message.toAdvancedMessage().annotations());
    }

    if (message.hasFooters()) {
      writeObject(headers, "footers", message.toAdvancedMessage().footer());
    }
    return headers;
  }
}
