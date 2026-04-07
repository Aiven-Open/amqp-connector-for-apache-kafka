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
