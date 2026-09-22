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

public abstract class AbstractAmqpStrategy implements Strategy {
  protected final EncoderDecoder converter = AmqpCommonConfig.getCommonConverter();
  protected final Sender sender;
  protected final ErrantRecordHandler errantRecordHandler;
  @VisibleForTesting final ConcurrentSkipListMap<KafkaRecordKey, TrackerSinkRecord> commitMap;

  protected AbstractAmqpStrategy(Sender sender, ErrantRecordHandler errantRecordHandler) {
    this.sender = sender;
    this.errantRecordHandler = errantRecordHandler;
    commitMap = new ConcurrentSkipListMap<>();
  }

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
