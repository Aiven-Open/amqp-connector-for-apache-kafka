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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CollectionConverterTest {

  Converter underTest =
      new Converter.ChainedConverter(
          new KafkaConverter(), new CollectionConverter(), new UniqueTypeConverter());

  @ParameterizedTest
  @MethodSource("arrayTestData")
  void arrayTest(Object expected1, Object expected2) {
    Object[] expected = {expected1, expected2};
    SchemaAndValue encoded = assertThat(underTest.encode(expected)).isPresent().get().actual();
    assertThat(encoded.value()).isInstanceOf(Struct.class);
    Object decoded = assertThat(underTest.decode(encoded)).isPresent().get().actual();
    assertThat(decoded.getClass().isArray()).isTrue();
    Object[] ary = (Object[]) decoded;
    assertThat(ary).containsExactly(expected);
  }

  static List<Arguments> arrayTestData() {
    List<Arguments> result =
        List.of(
            Arguments.of(1, 2),
            Arguments.of("hello", "world"),
            Arguments.of(1L, 2L),
            Arguments.of((byte) 1, (byte) 2),
            Arguments.of((short) 1, (short) 2),
            Arguments.of(new ULID().nextValue(), UUID.randomUUID()));
    return result;
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
