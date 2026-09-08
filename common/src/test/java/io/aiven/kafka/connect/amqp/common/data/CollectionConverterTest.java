package io.aiven.kafka.connect.amqp.common.data;

import static org.assertj.core.api.Assertions.assertThat;

import de.huxhorn.sulky.ulid.ULID;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

public class CollectionConverterTest {

  Converter underTest =
      new Converter.ChainedConverter(
          new KafkaConverter(), new CollectionConverter(), new UniqueTypeConverter());

  @Test
  void arrayTest() {
    ULID.Value ulid = new ULID().nextValue();
    UUID uuid = UUID.randomUUID();
    Object[] expected = new Object[] {uuid, ulid};
    SchemaAndValue encoded = assertThat(underTest.encode(expected)).isPresent().get().actual();
    assertThat(encoded.value()).isInstanceOf(Struct.class);
    Object decoded = assertThat(underTest.decode(encoded)).isPresent().get().actual();
    assertThat(decoded.getClass().isArray()).isTrue();
    Object[] ary = (Object[]) decoded;
    assertThat(ary).containsExactly(expected);
  }

  @Test
  void listTest() {
    List<Number> expected = List.of(1, 3.14);
    SchemaAndValue encoded = assertThat(underTest.encode(expected)).isPresent().get().actual();
    assertThat(encoded.value()).isInstanceOf(Struct.class);
    Object decoded = assertThat(underTest.decode(encoded)).isPresent().get().actual();
    List<?> result = (List<?>) assertThat(decoded).isInstanceOf(List.class).actual();
    assertThat(result).hasSameSizeAs(expected);
    for (int i = 0; i < expected.size(); i++) {
      assertThat(result.get(i)).isEqualTo(expected.get(i));
    }
  }

  @Test
  void MapTest() {
    Map<Number, Object> expected = Map.of(1, "One is the lonelest Number", 3.14, UUID.randomUUID());
    SchemaAndValue encoded = assertThat(underTest.encode(expected)).isPresent().get().actual();
    assertThat(encoded.value()).isInstanceOf(Struct.class);
    Object decoded = assertThat(underTest.decode(encoded)).isPresent().get().actual();
    Map<String, Object> map =
        (Map<String, Object>) assertThat(decoded).isInstanceOf(Map.class).actual();
    Map<String, Object> expMap = new LinkedHashMap<>();
    expected.forEach((k, v) -> expMap.put(k.toString(), v));
    assertThat(map).containsExactlyEntriesOf(expMap);
  }
}
