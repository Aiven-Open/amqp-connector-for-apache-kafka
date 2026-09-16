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
package io.aiven.kafka.connect.amqp.source;

import com.google.common.annotations.VisibleForTesting;
import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.NativeSourceData;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.kafka.connect.amqp.common.config.AmqpCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpHeaderProperties;
import io.aiven.kafka.connect.amqp.common.data.Converter;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Headers;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AMQP NativeSourceData implementation.
 *
 * <p>This implementation reads {@link Delivery} records from a protonj2 {@link Receiver} and
 * creates {@link AmqpSourceNativeInfo} objects from those.
 *
 * <p>Since {@link org.apache.qpid.protonj2.client.Message} objects are not required to have a
 * unique ID, this implementation uses a {@link ULID} for the native key. ULIDs are generated in the
 * {@link AmqpSourceNativeInfo} class.
 */
public final class AmqpSourceData extends NativeSourceData<String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSourceData.class);

  private final Receiver receiver;

  /** The maximum number of Deliveries to pull from the Receiver. */
  private final int receiveLimit;

  private final int taskId;

  private final Converter dataConverter;

  /**
   * Constructor.
   *
   * @param sourceConfig The AMQP Source configuration.
   * @param offsetManager the OffsetManager to use.
   * @throws ClientException on error.
   */
  AmqpSourceData(final AmqpSourceConfig sourceConfig, final OffsetManager offsetManager)
      throws ClientException, ExecutionException, InterruptedException {
    super(sourceConfig, offsetManager);
    taskId = sourceConfig.getTaskId();
    this.receiver = sourceConfig.getReceiver();
    receiveLimit = 500; // TODO make this configurable
    dataConverter = AmqpCommonConfig.getCommonConverter();
  }

  @Override
  protected Function<EvolvingSourceRecord, EvolvingSourceRecord> initializeRecordFunction() {
    return this::initialize;
  }

  private void writeObject(Headers headers, String name, Object value) {
    if (value != null) {
      dataConverter
              .encode(value)
              .ifPresentOrElse(
                      schemaAndValue -> writeSchema(headers, name, schemaAndValue),
                      () -> LOGGER.warn("Unknown data type {} for {}", value.getClass(), name));
    }
  }

  private void writeSchema(Headers headers, String name, SchemaAndValue schemaAndValue) {
    headers.add("amqp." + name, schemaAndValue);
  }

  private void setKeyValue(final EvolvingSourceRecord result, final String defaultKey) {
    dataConverter.encode(result.getContext().getNativeKey()).ifPresentOrElse(result::setKeyData,
            () -> {
              LOGGER.error(
                      "Unexpected data type in native key {}.  Using: {}", result.getContext().getNativeKey().getClass(),
                      defaultKey);
              result.setKeyData(new SchemaAndValue(Schema.STRING_SCHEMA, defaultKey));
            });
  }

  private Headers processHeaders(Headers headers, Message<?> message) throws ClientException {
    for (AmqpHeaderProperties property : AmqpHeaderProperties.values()) {
      switch (property) {
        case MESSAGE_ID -> writeObject(headers, property.getSchemaName(), message.messageId());
        case USER_ID -> writeObject(headers, property.getSchemaName(), message.to());
        case SUBJECT -> writeObject(headers, property.getSchemaName(), message.subject());
        case REPLY_TO -> writeObject(headers, property.getSchemaName(), message.replyTo());
        case CORRELATION_ID -> writeObject(headers, property.getSchemaName(), message.correlationId());
        case CONTENT_TYPE -> writeObject(headers, property.getSchemaName(), message.contentType());
        case CONTENT_ENCODING -> writeObject(headers, property.getSchemaName(), message.contentEncoding());
        case ABSOLUTE_EXPIRY -> writeSchema(
                headers,
                property.getSchemaName(),
                new SchemaAndValue(Schema.INT64_SCHEMA, message.absoluteExpiryTime()));
        case CREATION_TIME -> writeSchema(
                headers,
                property.getSchemaName(),
                new SchemaAndValue(Schema.INT64_SCHEMA, message.creationTime()));
        case GROUP_ID -> writeObject(
                headers,
                property.getSchemaName(),
                new SchemaAndValue(Schema.INT32_SCHEMA, message.groupId()));
        case GROUP_SEQUENCE -> writeSchema(
                headers,
                property.getSchemaName(),
                new SchemaAndValue(Schema.INT32_SCHEMA, message.groupSequence()));
        case REPLY_TO_GROUP_ID -> writeObject(headers, property.getSchemaName(), message.replyToGroupId());
        case DURABLE -> writeSchema(
                headers,
                property.getSchemaName(),
                new SchemaAndValue(Schema.BOOLEAN_SCHEMA, message.durable()));
        case FIRST_ACQUIRER -> writeSchema(
                headers,
                property.getSchemaName(),
                new SchemaAndValue(Schema.BOOLEAN_SCHEMA, message.firstAcquirer()));
        case DELIVERY_COUNT -> writeSchema(
                headers,
                property.getSchemaName(),
                new SchemaAndValue(Schema.INT64_SCHEMA, message.deliveryCount()));
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

  private void processBody(final EvolvingSourceRecord result, List<Section<?>> body ) {
    // valid body types are Data (byte[]), AmqpSequence: (List<>), AmqpValue, but if we just pass
    // the section values they should encode correctly
    if (!body.isEmpty()) {
      Optional<SchemaAndValue> schemaAndValue =
      body.size() == 1 ?
        dataConverter.encode(body.get(0).getValue()) :
      dataConverter.encode(body.stream().map(Section::getValue).toList());

      schemaAndValue.ifPresentOrElse(
              result::setValueData,
              () ->
                      LOGGER.error(
                              "Unexpected data type in body {}",
                              String.join(", ", body.stream().map(Section::toString).toList())));
    }
  }
  /**
   * Converts the message internals into headers.
   *
   * @param record The initial EvolvingSourceRecord to initialize
   * @return an initialized record.  May be the same or different instance.
   */
  @VisibleForTesting
  EvolvingSourceRecord initialize(EvolvingSourceRecord record) {
    AmqpSourceNativeInfo sourceNativeInfo = record.getSourceNativeInfo();
    EvolvingSourceRecord result = record;
    try {
      Message<?> message = sourceNativeInfo.getMessage();
      // if the messageID is provided use it for the context which sets the default key
      // may change the result object.
      if (message.messageId() != null) {
        AmqpContext ctxt =
            ((AmqpContext) record.getContext()).builder().nativeKey(message.messageId().toString()).build();
        result =
            new EvolvingSourceRecord(
                sourceNativeInfo, createOffsetManagerEntry(ctxt), ctxt);
      }

      // start setting result values

      setKeyValue(result, sourceNativeInfo.nativeKey());

      result.setHeaders(processHeaders(record.getHeaders(), message));

      processBody(result, new ArrayList<>(message.toAdvancedMessage().bodySections()));

    } catch (ClientException e) {
      LOGGER.error("unable to extract message: {}", e.getMessage(), e);
    }

    return result;
  }

  @Override
  public String getSourceName() {
    return "AMQP Source";
  }

  @Override
  public Iterator<AmqpSourceNativeInfo> getNativeItemIterator(String ignore) {
    try {
      long waiting = receiver.queuedDeliveries();
      int limit = (int) Math.min(waiting, receiveLimit);
      List<AmqpSourceNativeInfo> lst = new ArrayList<>(limit);
      try {
        for (int i = 0; i < limit; i++) {
          Delivery delivery = receiver.tryReceive();
          if (delivery != null) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("task {}: read {}", taskId, delivery.message());
            }
            lst.add(new AmqpSourceNativeInfo(delivery));
          }
        }
      } catch (ClientException e) {
        LOGGER.warn("task {}: Client exception retrieving delivery: {}", taskId, e.getMessage(), e);
        // do nothing.
      }

      return lst.iterator();
    } catch (ClientException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public OffsetManager.OffsetManagerEntry createOffsetManagerEntry(Map<String, Object> data) {
    return new AmqpOffsetManagerEntry(data);
  }

  @Override
  protected OffsetManager.OffsetManagerEntry createOffsetManagerEntry(Context context) {
    return new AmqpOffsetManagerEntry((String) context.getNativeKey());
  }

  @Override
  protected Optional<KeySerde<String>> getNativeKeySerde() {
    return Optional.of(KeySerde.STRING_SERDE);
  }

  @Override
  public OffsetManager.OffsetManagerKey getOffsetManagerKey(String nativeKey) {
    return new AmqpOffsetManagerEntry(nativeKey).getManagerKey();
  }

  @Override
  public void close() throws Exception {
    super.close();
    try (receiver) {
      LOGGER.info("Closing the open receiver");
    }
  }
}
