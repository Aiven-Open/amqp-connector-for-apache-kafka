package io.aiven.kafka.connect.amqp.source.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.transformer.SchemaAndValueFactory;
import io.aiven.commons.kafka.connector.source.transformer.Transformer;
import io.aiven.commons.kafka.connector.source.transformer.TransformerInfo;
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
import org.apache.qpid.protonj2.types.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AmqpTransformer extends Transformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpTransformer.class);

  private final ObjectMapper objectMapper;
  private final JsonConverter jsonConverter;

  public AmqpTransformer(final SourceCommonConfig config) {
    super(config, info());
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(Message.class, new MessageSerializer());
    module.addSerializer(Section.class, new AmqpSectionSerializer());
    objectMapper.registerModule(module);

    jsonConverter = new JsonConverter();
    jsonConverter.configure(Map.of("schemas.enable", "false"), false);
  }

  public static TransformerInfo info() {
    return new TransformerInfo(
        "Amqp",
        AmqpTransformer.class,
        TransformerInfo.FEATURE_NONE,
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
