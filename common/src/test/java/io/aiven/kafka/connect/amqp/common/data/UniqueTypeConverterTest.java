package io.aiven.kafka.connect.amqp.common.data;

import static org.assertj.core.api.Assertions.assertThat;

import de.huxhorn.sulky.ulid.ULID;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

public class UniqueTypeConverterTest {
  private final UniqueTypeConverter underTest = new UniqueTypeConverter();

  @Test
  void uuidTest() {
    UUID expectedValue = UUID.randomUUID();
    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.STRING).name(UUID.class.getCanonicalName()).build();

    Optional<SchemaAndValue> result = underTest.encode(expectedValue);
    SchemaAndValue sv = assertThat(result).isPresent().get().actual();
    assertThat(sv.schema()).isEqualTo(expectedSchema);
    assertThat(sv.value()).isInstanceOf(String.class).isEqualTo(expectedValue.toString());

    Optional<Object> object = underTest.decode(result.get());
    UUID decoded = (UUID) assertThat(object).isPresent().get().isInstanceOf(UUID.class).actual();
    assertThat(decoded).isEqualTo(expectedValue);
  }

  @Test
  void ulidTest() {
    ULID ulid = new ULID();
    ULID.Value expectedValue = ulid.nextValue();

    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.STRING).name(ULID.Value.class.getCanonicalName()).build();

    Optional<SchemaAndValue> result = underTest.encode(expectedValue);
    SchemaAndValue sv = assertThat(result).isPresent().get().actual();
    assertThat(sv.schema()).isEqualTo(expectedSchema);
    assertThat(sv.value()).isInstanceOf(String.class).isEqualTo(expectedValue.toString());

    Optional<Object> object = underTest.decode(result.get());
    ULID.Value decoded =
        (ULID.Value) assertThat(object).isPresent().get().isInstanceOf(ULID.Value.class).actual();
    assertThat(decoded).isEqualTo(expectedValue);
  }

  @Test
  void notUniqueTypeTest() {
    assertThat(underTest.encode(6L)).isNotPresent();
    assertThat(underTest.decode(new SchemaAndValue(Schema.STRING_SCHEMA, "foo"))).isNotPresent();
    assertThat(underTest.encode(null)).isNotPresent();
  }
}
