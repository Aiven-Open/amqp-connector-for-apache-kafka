package io.aiven.kafka.connect.amqp.source;

import io.aiven.commons.kafka.connector.source.NativeSourceData;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class AmqpSourceData implements NativeSourceData<AmqpNativeInfo, AmqpNativeInfo, AmqpOffsetManagerEntry, AmqpSourceRecord> {
	private final Receiver receiver;
	private final int streamLimit;

	AmqpSourceData(AmqpSourceConfig sourceConfig) {
		this.receiver = sourceConfig.getReceiver();
		streamLimit = sourceConfig.getStreamLimit();
	}

	@Override
	public String getSourceName() {
		return "AMQP Source";
	}

	@Override
	public Stream<AmqpNativeInfo> getNativeItemStream(AmqpNativeInfo ignore) {
		try {
			long waiting = receiver.queuedDeliveries();
			int limit = (int) Math.min(waiting, streamLimit);
			List<AmqpNativeInfo> lst = new ArrayList<>(limit);
			try {
				for (int i = 0; i < limit; i++) {
					Delivery delivery = receiver.tryReceive();
					if (delivery != null) {
						lst.add(new AmqpNativeInfo(delivery));
					}
				}
			} catch (ClientException e) {
				// do nothing.
			}

			return lst.stream();
		} catch (ClientException e) {
			throw new ConnectException(e);
		}
	}

	@Override
	public IOSupplier<InputStream> getInputStream(AmqpSourceRecord amqpNativeRecord) {
		return () -> {
            try {
                return amqpNativeRecord.getNativeItem().getDelivery().rawInputStream();
            } catch (ClientException e) {
                throw new IOException(e);
            }
        };
	}



	@Override
	public AmqpNativeInfo getNativeKey(AmqpNativeInfo amqpNativeInfo) {
		return amqpNativeInfo;
	}

	@Override
	public AmqpNativeInfo parseNativeKey(String keyString) {
		return null;
	}

	@Override
	public AmqpSourceRecord createSourceRecord(AmqpNativeInfo amqpNativeInfo) {
		return new AmqpSourceRecord(amqpNativeInfo, createOffsetManagerEntry(amqpNativeInfo));
	}

	@Override
	public AmqpOffsetManagerEntry createOffsetManagerEntry(AmqpNativeInfo amqpNativeInfo) {
		return new AmqpOffsetManagerEntry(amqpNativeInfo.getULID());
	}

	@Override
	public OffsetManager.OffsetManagerKey getOffsetManagerKey(AmqpNativeInfo amqpNativeInfo) {
		return new AmqpOffsetManagerEntry(amqpNativeInfo.getULID()).getManagerKey();
	}

	@Override
	public Optional<Context<AmqpNativeInfo>> extractContext(AmqpNativeInfo amqpNativeInfo) {
		return Optional.empty();
	}
}
