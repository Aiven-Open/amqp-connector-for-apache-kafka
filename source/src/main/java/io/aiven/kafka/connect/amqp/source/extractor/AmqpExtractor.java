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
package io.aiven.kafka.connect.amqp.source.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.extractor.Extractor;
import io.aiven.commons.kafka.connector.source.extractor.ExtractorInfo;
import io.aiven.commons.kafka.connector.source.extractor.SchemaAndValueFactory;
import io.aiven.commons.util.io.compression.CompressionType;
import io.aiven.kafka.connect.amqp.source.AmqpSourceNativeInfo;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Decimal128;
import org.apache.qpid.protonj2.types.Decimal32;
import org.apache.qpid.protonj2.types.Decimal64;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Extracts data from the AMQP Message. Each AMQP message generates a single Kafka message */
public final class AmqpExtractor extends Extractor {


  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpExtractor.class);

  private final ObjectMapper objectMapper;
  private final JsonConverter jsonConverter;

  /**
   * Creates AmqpExtractor
   *
   * @param config the
   */
  public AmqpExtractor(final SourceCommonConfig config) {
    super(config, info());
    objectMapper = registerSerializers(new ObjectMapper());
    jsonConverter = new JsonConverter();
    jsonConverter.configure(Map.of("schemas.enable", "false"), false);
  }

  public static ObjectMapper registerSerializers(final ObjectMapper objectMapper) {
    SimpleModule module = new SimpleModule();
    module.addSerializer(Message.class, new MessageSerializer());
    module.addSerializer(Section.class, new AmqpSectionSerializer());
    module.addSerializer(Binary.class, new AmqpBinarySerializer());
    module.addSerializer(Symbol.class, new AmqpSymbolSerializer());
    module.addSerializer(Decimal64.class, new AmqpDecimal64Serializer());
    module.addSerializer(new AmqpDecimal32Serializer());
    module.addSerializer(Decimal128.class, new AmqpDecimal128Serializer());
    module.addSerializer(Symbol.class, new AmqpSymbolSerializer());
    objectMapper.registerModule(module);
    return objectMapper;
  }

  /**
   * Creates an ExtractorInfo for this extractor.
   *
   * @return the ExtractorInfo for this extractor.
   */
  public static ExtractorInfo info() {
    return new ExtractorInfo(
        "Amqp",
        AmqpExtractor.class,
        ExtractorInfo.FEATURE_NONE,
        "Processes an AMQP message.  All message features are presented in a Json document.  Message body is uncompressed if specified");
  }

  @Override
  public Stream<SchemaAndValue> generateRecords(EvolvingSourceRecord sourceRecord) {
    return generateValue((AmqpSourceNativeInfo) sourceRecord.getSourceNativeInfo()).stream();
  }

  @Override
  public SchemaAndValue generateKeyData(EvolvingSourceRecord evolvingSourceRecord) {
    return SchemaAndValueFactory.createSchemaAndValue(evolvingSourceRecord.getNativeKey());
  }

  private Optional<SchemaAndValue> generateValue(AmqpSourceNativeInfo nativeInfo) {
    try {
      Message<Object> message = nativeInfo.getMessage();
      if (config.getCompressionType() != CompressionType.NONE) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (InputStream inputStream =
            config.getCompressionType().decompress(nativeInfo.getInputStream())) {
          IOUtils.copy(inputStream, baos);
        }
        message.body(baos.toByteArray());
      }
      return Optional.of(
          jsonConverter.toConnectData(null, objectMapper.writeValueAsBytes(message)));
    } catch (ClientException | IOException e) {
      LOGGER.error("Error reading input stream: {}", e.getMessage(), e);
      return Optional.empty();
    }
  }

  @Override
  public void close() {
    jsonConverter.close();
  }
}
