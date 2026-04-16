package io.aiven.kafka.connect.amqp.source;

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigFragment;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.SourceStorage;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import io.aiven.kafka.connect.amqp.common.integration.IntegrationTestSetup;
import io.aiven.kafka.connect.amqp.source.extractor.AmqpExtractor;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Delivery;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import io.aiven.commons.kafka.connector.source.extractor.ExtractorRegistry;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.testcontainers.rabbitmq.RabbitMQContainer;

public class AmqpSourceStorage implements SourceStorage<ULID.Value, Delivery> {
    private static final ExtractorRegistry extractorRegistry = ExtractorRegistry.builder().add(AmqpExtractor.info()).build();

    private final RabbitMQContainer rabbit;
    private final Client client;
    private final Connection connection;
    private Sender sender;
    private Receiver receiver;
    private String amqpAddress;

    public AmqpSourceStorage(RabbitMQContainer rabbit) throws ClientException {
        this.rabbit = rabbit;
        client = Client.create();
        connection = client.connect(rabbit.getHost(), rabbit.getAmqpPort(),
                new ConnectionOptions()
                        .user(rabbit.getAdminUsername())
                        .password(rabbit.getAdminUsername()));
    }

    @Override
    public ExtractorRegistry supportedExtractors() {
        return extractorRegistry;
    }

    @Override
    public ULID.Value createKey(String topic, int partition) {
        return AmqpSourceNativeInfo.nextValue();
    }

    @Override
    public WriteResult<ULID.Value> writeWithKey(ULID.Value nativeKey, byte[] testDataBytes) {
        try {
        Message<byte[]> message = Message.create(testDataBytes).messageId(nativeKey.toString());
            sender.send(message);
            return new WriteResult<>(null, nativeKey);
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }
    }

    public void setAmqpAddress(String address) {
        this.amqpAddress = address;
    }

    @Override
    public Map<String, String> createConnectorConfig() {
        Map<String, String> data = new HashMap<>();
        AmqpFragment.setter(data)
                .setHost(rabbit.getHost())
                .setPort(rabbit.getAmqpPort())
                .setAddress(amqpAddress)
                .setUser("guest")
                .setPassword("guest");
        return data;
    }

    @Override
    public BiFunction<Map<String, Object>, Map<String, Object>, OffsetManager.OffsetManagerEntry> offsetManagerEntryFactory() {
        return null;
    }

    @Override
    public Class<? extends Connector> getConnectorClass() {
        return AmqpSourceConnector.class;
    }

    @Override
    public void createStorage() {
        if (sender != null) {
            removeStorage();
        }
        try {
            sender = connection.openSender(amqpAddress);
            receiver = connection.openReceiver(amqpAddress);
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeStorage() {
        if (sender != null) {
            sender.close();
            sender = null;
        }
        if (receiver != null) {
            receiver.close();
            receiver = null;
        }
    }

    @Override
    public List<? extends NativeInfo<ULID.Value, Delivery>> getNativeInfo() {
        if (receiver != null) {
            try {
                int limit = (int) Math.min(Integer.MAX_VALUE, receiver.queuedDeliveries());
                if (limit > 0) {
                    List<NativeInfo<ULID.Value, Delivery>> lst = new ArrayList<>();

                    for (int i = 0; i < limit; i++) {
                        NativeInfo<ULID.Value, Delivery> ni = new NativeInfo<>(AmqpSourceNativeInfo.nextValue(), receiver.receive());
                        lst.add(ni);
                    }
                    return lst;
                }
                return Collections.emptyList();
            } catch (ClientException e) {
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("Receiver not open");
    }

    @Override
    public IOSupplier<InputStream> getInputStream(ULID.Value nativeKey) {
        return null;
    }

    @Override
    public String defaultPrefix() {
        return "";
    }
}
