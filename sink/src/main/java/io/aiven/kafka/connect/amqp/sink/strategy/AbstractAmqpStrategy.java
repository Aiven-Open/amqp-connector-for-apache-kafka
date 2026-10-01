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

import com.google.common.annotations.VisibleForTesting;
import io.aiven.kafka.connect.amqp.common.AmqpParseException;
import io.aiven.kafka.connect.amqp.common.KafkaRecordKey;
import io.aiven.kafka.connect.amqp.common.config.AmqpCommonConfig;
import io.aiven.kafka.connect.amqp.common.data.EncoderDecoder;
import io.aiven.kafka.connect.amqp.sink.errant.ErrantRecordHandler;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.LoggerFactory;

/**
 * Base implementation for AMQP strategies. Handles reporting when messages have been confirmed as
 * sent to AMQP destination.
 */
public abstract class AbstractAmqpStrategy implements Strategy {
  /** The Encoder/Decoder to use */
  protected final EncoderDecoder encoderDecoder = AmqpCommonConfig.getCommonConverter();

  /** The sender to send AMQP messages */
  protected final Sender sender;

  /** The handler for failed messages */
  protected final ErrantRecordHandler errantRecordHandler;

  /** The map of KafkaRecordKeys to Trackers */
  @VisibleForTesting final ConcurrentSkipListMap<KafkaRecordKey, TrackerSinkRecord> commitMap;

  /**
   * Constructs a strategy with the specified sender and errant record handler.
   *
   * @param sender the Sender to use.
   * @param errantRecordHandler The errant record handler to use.
   */
  protected AbstractAmqpStrategy(Sender sender, ErrantRecordHandler errantRecordHandler) {
    this.sender = sender;
    this.errantRecordHandler = errantRecordHandler;
    commitMap = new ConcurrentSkipListMap<>();
  }

  /**
   * Creates an AMQP message that contains the data from the sink record.
   *
   * @param sinkRecord the sink record to extract data from.
   * @return a populated AMQP message.
   * @throws AmqpParseException on AMQP data error.
   * @throws ClientException on AMQP connection error.
   */
  abstract Message<?> createClientMessage(SinkRecord sinkRecord)
      throws AmqpParseException, ClientException;

  @Override
  public void write(SinkRecord sinkRecord) {
    try {
      Message<?> message = createClientMessage(sinkRecord);
      Tracker tracker = sender.send(message);
      Future<Tracker> futureTracker = tracker.settlementFuture();
      commitMap.put(
          new KafkaRecordKey(sinkRecord), new TrackerSinkRecord(futureTracker, sinkRecord));
    } catch (AmqpParseException | ClientException e) {
      errantRecordHandler.reportErrantRecord(sinkRecord, e);
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
    currentOffsets.forEach(
        (tp, om) -> {
          KafkaRecordKey key = new KafkaRecordKey(tp, om.offset());
          KafkaRecordKey highestNoBreak = null;
          boolean sawBreak = false;
          /* check the key range remove all completed records before the suggested offset.  If there are any remaining return the
          record that occurred just before that.  If it is the first entry then don't commit any for that topic/partition pair.
          */
          ConcurrentNavigableMap<KafkaRecordKey, TrackerSinkRecord> subMap =
              commitMap.subMap(new KafkaRecordKey(tp, 0), true, key, true);
          for (Map.Entry<KafkaRecordKey, TrackerSinkRecord> entry : subMap.entrySet()) {
            if (entry.getValue().trackerFuture.isDone()) {
              if (entry.getValue().trackerFuture.isCancelled()) {
                errantRecordHandler.reportErrantRecord(
                    entry.getValue().sinkRecord, "Delivery cancelled");
              }
              subMap.remove(entry.getKey());
              if (!sawBreak) {
                highestNoBreak = entry.getKey();
              }
            } else {
              sawBreak = true;
            }
          }

          /* default case in this if block is not to commit any messages for the topic/partition. */
          if (subMap.isEmpty()) {
            result.put(tp, om);
          } else if (highestNoBreak != null) {
            result.put(
                new TopicPartition(highestNoBreak.topic(), highestNoBreak.partition()),
                new OffsetAndMetadata(highestNoBreak.offset()));
          }
        });
    return result;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    LoggerFactory.getLogger(this.getClass())
        .error("flush() Should not be called from the AmqpBodyFmt strategy");
  }

  /**
   * A mapping for AMQP trackers to SinkRecords.
   *
   * @param trackerFuture the tracker that will report when the write is acknowledged.
   * @param sinkRecord the record that generated the write.
   */
  public record TrackerSinkRecord(Future<Tracker> trackerFuture, SinkRecord sinkRecord) {}
}
