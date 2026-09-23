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

import io.aiven.kafka.connect.amqp.sink.errant.ErrantRecordHandler;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/** Definition of a write strategy. */
public interface Strategy {

  /**
   * Writes a single sinkRecord. Any errors encountered in writing should be reported via an {@link
   * ErrantRecordHandler}.
   *
   * @param sinkRecord the sink record to write.
   */
  void write(SinkRecord sinkRecord);

  /**
   * Writes multiple SinkRecords. Any errors encountered in writing should be reported via an {@link
   * ErrantRecordHandler}. By default, this method iterates over the collection calling {@link
   * #write(SinkRecord)}. This method can be overridden for strategies that handle bulk writing
   * and/or want to report bulk errant messages.
   *
   * @param records the records to write.
   */
  default void write(Collection<SinkRecord> records) {
    records.forEach(this::write);
  }

  /**
   * Adjust the currentOffsets to commit. This method should return the highest offset for the
   * topic/partition that should be committed. By default, this method calls {@link #flush} and
   * returns the entire list of candidates that kafka is suggesting. This is the default behavior
   * for a {@link SinkTask}.
   *
   * @param currentOffsets the offsets to commit.
   * @return a map of committable topic/partition/offset values.
   */
  default Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    flush(currentOffsets);
    return currentOffsets;
  }

  /**
   * Flushes the data to the storage. For strategies that stream the data there may not be a need
   * for a {@code flush} method in which case this method amy be a no-op.
   *
   * @param currentOffsets the highest offset for each topic/partition that should be written.
   */
  void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets);
}
