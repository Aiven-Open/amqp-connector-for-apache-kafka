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

import static org.assertj.core.api.Assertions.assertThat;

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class AmqpOffsetManagerEntryTest {
  ULID.Value primaryKey = new ULID().nextValue();

  @Test
  void constructorTest() {
    AmqpOffsetManagerEntry entry = new AmqpOffsetManagerEntry(primaryKey);
    OffsetManager.OffsetManagerKey key = entry.getManagerKey();
    Map<String, Object> keyMap = key.getPartitionMap();
    assertThat(keyMap)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("ulid", primaryKey.toString(), "recordCount", 0));
    Map<String, Object> map = entry.getProperties();
    assertThat(map)
        .containsExactlyInAnyOrderEntriesOf(Map.of("ulid", primaryKey, "recordCount", 0));
  }

  @Test
  void incrementRecordCount() {
    AmqpOffsetManagerEntry entry = new AmqpOffsetManagerEntry(primaryKey);
    OffsetManager.OffsetManagerKey key = entry.getManagerKey();
    Map<String, Object> map = key.getPartitionMap();
    assertThat(map)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("ulid", primaryKey.toString(), "recordCount", 0));
    map = entry.getProperties();
    assertThat(map)
        .containsExactlyInAnyOrderEntriesOf(Map.of("ulid", primaryKey, "recordCount", 0));

    entry.incrementRecordCount();

    map = entry.getManagerKey().getPartitionMap();
    assertThat(map)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("ulid", primaryKey.toString(), "recordCount", 1));
    map = entry.getProperties();
    assertThat(map)
        .containsExactlyInAnyOrderEntriesOf(Map.of("ulid", primaryKey, "recordCount", 1));
  }
}
