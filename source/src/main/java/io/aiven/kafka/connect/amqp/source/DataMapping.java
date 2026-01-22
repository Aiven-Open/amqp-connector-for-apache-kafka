// package io.aiven.kafka.connect.amqp.source;
//
// import org.apache.kafka.connect.data.Schema;
// import org.apache.qpid.protonj2.types.Binary;
// import org.apache.qpid.protonj2.types.Decimal128;
// import org.apache.qpid.protonj2.types.Decimal32;
// import org.apache.qpid.protonj2.types.Decimal64;
// import org.apache.qpid.protonj2.types.UnsignedByte;
// import org.apache.qpid.protonj2.types.UnsignedInteger;
// import org.apache.qpid.protonj2.types.UnsignedLong;
// import org.apache.qpid.protonj2.types.UnsignedShort;
//
// import java.math.BigInteger;
// import java.util.function.Function;
//
// public enum DataMapping {
//
// BINARY(Binary.class, byte[].class),
// DECIMAL_32(Decimal32.class, Schema.INT32_SCHEMA),
// DECIMAL_64(Decimal64.class, Schema.INT64_SCHEMA),
// DECIMAL_128(Decimal128.class, Schema.INTBigInteger.class),
// UNSIGNED_BYTE(UnsignedByte.class, byte[].class),
// UNSIGNED_INTEGER(UnsignedInteger.class, Integer.class),
// UNSIGNED_LONG(UnsignedLong.class, Long.class),
// UNSIGNED_SHORT(UnsignedShort.class, Short.class),
//
// STRING(String.class, String.class),
//
//
// private Class<?> amqpType;
// private Class<?> kafkaType;
// private Function<Object, Schema> amqpToKafka;
//
// DataMapping(Class<?> amqpType, Schema kafkaType/*, Function<Object, Object>
// amqpToKafka*/) {
// this.amqpType = amqpType;
// this.kafkaType = kafkaType;
// this.amqpToKafka = amqpToKafka;
// }
//
// }
