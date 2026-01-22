package io.aiven.kafka.connect.amqp.source;

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.transformer.Transformer;
import org.apache.qpid.protonj2.client.Delivery;

/**
 * An object that is its own key and data.
 */
public class AmqpNativeInfo implements Comparable<AmqpNativeInfo>, NativeInfo<AmqpNativeInfo, AmqpNativeInfo> {
    private static final ULID ulid = new ULID();

    private final Delivery delivery;
    private final ULID.Value value;

    public AmqpNativeInfo(Delivery delivery) {
        this.delivery = delivery;
        this.value = ulid.nextValue();
    }

    public Delivery getDelivery() {
        return delivery;
    }

    public ULID.Value getULID() {
        return value;
    }

    @Override
    public int compareTo(AmqpNativeInfo other) {
        return value.compareTo(other.value);
    }

    @Override
    public AmqpNativeInfo getNativeItem() {
        return this;
    }

    @Override
    public AmqpNativeInfo getNativeKey() {
        return this;
    }

    @Override
    public long getNativeItemSize() {
        return Transformer.UNKNOWN_STREAM_LENGTH;
    }
}
