// package io.aiven.kafka.connect.amqp.source;
//
// import org.apache.kafka.connect.data.Schema;
// import org.apache.qpid.protonj2.client.Message;
//
// import java.util.List;
//
// public class AmqpRecordConverter {
//
// public AmqpRecordConverter() {
//
// }
//
// private Object convertObject(Object kafkaConnectObject, Schema
// kafkaConnectSchema) {
// if (kafkaConnectObject == null) {
// if (kafkaConnectSchema.isOptional()) {
// // short circuit converting the object
// return null;
// } else {
// throw new ConversionConnectException(
// kafkaConnectSchema.name() + " is not optional, but converting object had null
// value");
// }
// }
//
// LogicalTypeConverter converter =
// LogicalConverterRegistry.getConverter(kafkaConnectSchema.name());
// if (converter != null) {
// return converter.convert(kafkaConnectObject);
// }
// Schema.Type kafkaConnectSchemaType = kafkaConnectSchema.type();
// switch (kafkaConnectSchemaType) {
// case ARRAY:
// return convertArray(kafkaConnectObject, kafkaConnectSchema);
// case MAP:
// return convertMap(kafkaConnectObject, kafkaConnectSchema);
// case STRUCT:
// return convertStruct(kafkaConnectObject, kafkaConnectSchema);
// case BYTES:
// return convertBytes(kafkaConnectObject);
// case FLOAT64:
// return convertDouble((Double) kafkaConnectObject);
//
// case FLOAT32:
// return useStorageWriteApi
// ? ((Float) kafkaConnectObject).doubleValue()
// : convertFloat(kafkaConnectObject);
// case INT8:
// return useStorageWriteApi
// ? ((Byte) kafkaConnectObject).intValue()
// : kafkaConnectObject;
// case INT16:
// return useStorageWriteApi
// ? ((Short) kafkaConnectObject).intValue()
// : kafkaConnectObject;
//
// case BOOLEAN:
// case INT32:
// case INT64:
// case STRING:
// return kafkaConnectObject;
// default:
// throw new ConversionConnectException("Unrecognized schema type: " +
// kafkaConnectSchemaType);
// }
// }
// }
