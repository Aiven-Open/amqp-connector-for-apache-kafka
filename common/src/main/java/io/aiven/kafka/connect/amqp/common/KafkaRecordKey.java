package io.aiven.kafka.connect.amqp.common;

import java.util.Comparator;
import java.util.Objects;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A record that is used to track offsets from Kafka. Generally sued in maps to track source or sink
 * records.
 *
 * @param topic the original topic.
 * @param partition the original partition.
 * @param offset the original offset.
 */
public record KafkaRecordKey(String topic, int partition, long offset)
    implements Comparable<KafkaRecordKey> {
  @Override
  public int compareTo(KafkaRecordKey that) {
    return Objects.compare(
        this,
        that,
        Comparator.comparing(KafkaRecordKey::topic)
            .thenComparing(KafkaRecordKey::partition)
            .thenComparing(KafkaRecordKey::offset));
  }

  /**
   * Creates an instance from a SinkRecord.
   *
   * @param sinkRecord the sink record to extract data from.
   */
  public KafkaRecordKey(SinkRecord sinkRecord) {
    this(
        sinkRecord.originalTopic(),
        sinkRecord.originalKafkaPartition(),
        sinkRecord.originalKafkaOffset());
  }

  /**
   * Creates an instance from a RecordMetadata instance.
   *
   * @param recordMetadata the record metadata to extract data from.
   */
  public KafkaRecordKey(RecordMetadata recordMetadata) {
    this(recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
  }

  /**
   * Creates an instance from a TopicPartition object and an offset.
   *
   * @param topicPartition the TopicPartition to use.
   * @param offset the offset.
   */
  public KafkaRecordKey(TopicPartition topicPartition, long offset) {
    this(topicPartition.topic(), topicPartition.partition(), offset);
  }
}
