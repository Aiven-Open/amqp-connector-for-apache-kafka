package io.aiven.kafka.connect.amqp.common.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class ConverterTest {

    @Test
    void chainedConverterTest() {
        Converter1 c1 = new Converter1();
        Converter2 c2 = new Converter2();


        Converter underTest = c1.andThen(c2);

        assertThat(underTest.encode(6)).isNotPresent();
        SchemaAndValue schemaAndValue = assertThat(underTest.encode(6L)).isPresent()
                .get().actual();
        assertThat(schemaAndValue.schema().name()).isEqualTo("converter1");
        assertThat(schemaAndValue.value()).isEqualTo("6");

        assertThat(underTest.decode(new SchemaAndValue(Converter1.schema, "7")))
                .isPresent().get().isInstanceOf(Long.class).isEqualTo(7L);

        assertThat(underTest.decode(new SchemaAndValue(Converter2.schema, "8")))
                .isPresent().get().isInstanceOf(Long.class).isEqualTo(8L);

        assertThat(underTest.decode(new SchemaAndValue(Schema.INT64_SCHEMA, 6L))).isNotPresent();

        underTest = c2.andThen(c1);

        assertThat(underTest.encode(7)).isNotPresent();
        SchemaAndValue schemaAndValue2 = assertThat(underTest.encode(6L)).isPresent()
                .get().actual();
        assertThat(schemaAndValue2.schema().name()).isEqualTo("converter2");
        assertThat(schemaAndValue2.value()).isEqualTo("6");

        assertThat(underTest.decode(new SchemaAndValue(Converter1.schema, "7")))
                .isPresent().get().isInstanceOf(Long.class).isEqualTo(7L);

        assertThat(underTest.decode(new SchemaAndValue(Converter2.schema, "8")))
                .isPresent().get().isInstanceOf(Long.class).isEqualTo(8L);

        assertThat(underTest.decode(new SchemaAndValue(Schema.INT64_SCHEMA, 6L))).isNotPresent();

    }

    static class Converter1 extends Converter {

        static Schema schema = new SchemaBuilder(Schema.Type.STRING).name("converter1").build();
        @Override
        public Optional<SchemaAndValue> encode(Object value) {
            if (value instanceof Long) {
                return Optional.of(new SchemaAndValue(schema, value.toString()));
            }
            return Optional.empty();
        }

        @Override
        public Optional<Object> decode(SchemaAndValue schemaAndValue) {
            if ("converter1".equals(schemaAndValue.schema().name())) {
                return Optional.of(Long.valueOf((String) schemaAndValue.value()));
            }
            return Optional.empty();
        }
    }

    static class Converter2 extends Converter {

        static Schema schema = new SchemaBuilder(Schema.Type.STRING).name("converter2").build();
        @Override
        public Optional<SchemaAndValue> encode(Object value) {
            if (value instanceof Long) {
                return Optional.of(new SchemaAndValue(schema, value.toString()));
            }
            return Optional.empty();
        }

        @Override
        public Optional<Object> decode(SchemaAndValue schemaAndValue) {
            if ("converter2".equals(schemaAndValue.schema().name())) {
                return Optional.of(Long.valueOf((String) schemaAndValue.value()));
            }
            return Optional.empty();
        }
    }
}
