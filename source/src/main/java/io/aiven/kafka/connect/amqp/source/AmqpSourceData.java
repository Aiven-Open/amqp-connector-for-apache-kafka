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
import io.aiven.kafka.connect.amqp.common.config.AmqpHeaderProperties;
import io.aiven.kafka.connect.amqp.common.data.AmqpConverter;
import io.aiven.kafka.connect.amqp.common.data.CollectionConverter;
import io.aiven.kafka.connect.amqp.common.data.Converter;
import io.aiven.kafka.connect.amqp.common.data.KafkaConverter;
import io.aiven.kafka.connect.amqp.common.data.UniqueTypeConverter;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
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
public final class AmqpSourceData extends NativeSourceData<ULID.Value> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSourceData.class);

  private static final ULIDSerde serde = new ULIDSerde();

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
    dataConverter =
        new Converter.ChainedConverter(
            new AmqpConverter(),
            new UniqueTypeConverter(),
            new KafkaConverter(),
            new CollectionConverter());
  }

  @Override
  protected Function<EvolvingSourceRecord, EvolvingSourceRecord> initializeRecordFunction() {
    return this::initialize;
  }

  private void writeObject(Headers headers, String name, Object value) {
    dataConverter
        .encode(value)
        .ifPresentOrElse(
            schemaAndValue -> writeSchema(headers, name, schemaAndValue),
            () -> LOGGER.warn("Unknown data type {} for {}", value.getClass(), name));
  }

  private void writeSchema(Headers headers, String name, SchemaAndValue schemaAndValue) {
    headers.add("amqp." + name, schemaAndValue);
  }

  /**
   * Converts the message internals into headers.
   *
   * @param record
   * @return
   */
  @VisibleForTesting
  EvolvingSourceRecord initialize(EvolvingSourceRecord record) {
    AmqpSourceNativeInfo sourceNativeInfo = record.getSourceNativeInfo();
    try {
      Message<?> message = sourceNativeInfo.getMessage();
      Headers headers = record.getHeaders();
      for (AmqpHeaderProperties property : AmqpHeaderProperties.values()) {
        switch (property) {
          case MESSAGE_ID -> writeObject(headers, property.getSchemaName(), message.messageId());
          case USER_ID -> writeObject(headers, property.getSchemaName(), message.to());
          case SUBJECT -> writeObject(headers, property.getSchemaName(), message.subject());
          case REPLY_TO -> writeObject(headers, property.getSchemaName(), message.replyTo());
          case CORRELATION_ID ->
              writeObject(headers, property.getSchemaName(), message.correlationId());
          case CONTENT_TYPE ->
              writeObject(headers, property.getSchemaName(), message.contentType());
          case CONTENT_ENCODING ->
              writeObject(headers, property.getSchemaName(), message.contentEncoding());
          case ABSOLUTE_EXPIRY ->
              writeSchema(
                  headers,
                  property.getSchemaName(),
                  new SchemaAndValue(Schema.INT64_SCHEMA, message.absoluteExpiryTime()));
          case CREATION_TIME ->
              writeSchema(
                  headers,
                  property.getSchemaName(),
                  new SchemaAndValue(Schema.INT64_SCHEMA, message.creationTime()));
          case GROUP_ID ->
              writeObject(
                  headers,
                  property.getSchemaName(),
                  new SchemaAndValue(Schema.INT32_SCHEMA, message.groupId()));
          case GROUP_SEQUENCE ->
              writeSchema(
                  headers,
                  property.getSchemaName(),
                  new SchemaAndValue(Schema.INT32_SCHEMA, message.groupSequence()));
          case REPLY_TO_GROUP_ID ->
              writeObject(headers, property.getSchemaName(), message.replyToGroupId());
          case DURABLE ->
              writeSchema(
                  headers,
                  property.getSchemaName(),
                  new SchemaAndValue(Schema.BOOLEAN_SCHEMA, message.durable()));
          case FIRST_ACQUIRER ->
              writeSchema(
                  headers,
                  property.getSchemaName(),
                  new SchemaAndValue(Schema.BOOLEAN_SCHEMA, message.firstAcquirer()));
          case DELIVERY_COUNT ->
              writeSchema(
                  headers,
                  property.getSchemaName(),
                  new SchemaAndValue(Schema.INT64_SCHEMA, message.deliveryCount()));
        }
      }

      if (message.hasAnnotations()) {
        // LinkedHashMap is used in QPIDD source.
        Map<Symbol, Object> annotations = new LinkedHashMap<>();
        message.forEachAnnotation((k, v) -> annotations.put(Symbol.valueOf(k), v));
        MessageAnnotations messageAnnotations = new MessageAnnotations(annotations);
        writeObject(headers, "annotations", messageAnnotations);
      }

      if (message.hasFooters()) {
        Map<Symbol, Object> footers = new LinkedHashMap<>();
        message.forEachFooter((k, v) -> footers.put(Symbol.valueOf(k), v));
        Footer messageFooter = new Footer(footers);
        writeObject(headers, "footers", messageFooter);
      }

      record.setHeaders(headers);

      // valid body types are Data (byte[]), AmqpSequence: (List<>), AmqpValue, but if we just pass
      // the section values they should encode correctly
      List<Section<?>> body = new ArrayList<>(message.toAdvancedMessage().bodySections());
      if (!body.isEmpty()) {
        Optional<SchemaAndValue> schemaAndValue =
            body.size() == 1
                ? dataConverter.encode(body.get(0).getValue())
                : dataConverter.encode(body.stream().map(Section::getValue).toList());
        schemaAndValue.ifPresentOrElse(
            record::setValueData,
            () ->
                LOGGER.error(
                    "Unexpected data type in body {}",
                    String.join(", ", body.stream().map(Section::toString).toList())));
      }
    } catch (ClientException e) {
      LOGGER.error("unable to extract message: {}", e.getMessage(), e);
    }
    return record;
  }

  @Override
  public String getSourceName() {
    return "AMQP Source";
  }

  @Override
  public Iterator<AmqpSourceNativeInfo> getNativeItemIterator(ULID.Value ignore) {
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
    return new AmqpOffsetManagerEntry((ULID.Value) context.getNativeKey());
  }

  @Override
  protected Optional<KeySerde<ULID.Value>> getNativeKeySerde() {
    return Optional.of(serde);
  }

  @Override
  public OffsetManager.OffsetManagerKey getOffsetManagerKey(ULID.Value nativeKey) {
    return new AmqpOffsetManagerEntry(nativeKey).getManagerKey();
  }

  @Override
  public void close() throws Exception {
    super.close();
    try (receiver) {
      LOGGER.info("Closing the open receiver");
    }
  }

  /** The AMQP native source data implementation of NativeSourceData.KeySerde. */
  public static class ULIDSerde implements NativeSourceData.KeySerde<ULID.Value> {

    /** Default constructor */
    public ULIDSerde() {}

    @Override
    public String toString(ULID.Value nativeKey) {
      return nativeKey.toString();
    }

    @Override
    public ULID.Value fromString(String nativeKeyString) {
      return ULID.parseULID(nativeKeyString);
    }
  }
}
