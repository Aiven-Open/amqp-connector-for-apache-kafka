package io.aiven.kafka.connect.amqp.source;

import io.aiven.commons.kafka.connector.source.AbstractSourceRecord;

public class AmqpSourceRecord extends AbstractSourceRecord<AmqpNativeInfo, AmqpNativeInfo, AmqpOffsetManagerEntry, AmqpSourceRecord> {
    private final AmqpNativeInfo amqpNativeInfo;
    private final AmqpOffsetManagerEntry offsetManagerEntry;

    public AmqpSourceRecord(AmqpNativeInfo amqpNativeInfo, AmqpOffsetManagerEntry offsetManagerEntry) {
        super(amqpNativeInfo);
        this.amqpNativeInfo = amqpNativeInfo;
        this.offsetManagerEntry = offsetManagerEntry;
    }

    @Override
    public AmqpSourceRecord duplicate() {
        return new AmqpSourceRecord(amqpNativeInfo, offsetManagerEntry);
    }
}
