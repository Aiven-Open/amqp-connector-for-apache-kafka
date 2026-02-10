/*
         Copyright 2026 Aiven Oy and project contributors

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing,
        software distributed under the License is distributed on an
        "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
        KIND, either express or implied.  See the License for the
        specific language governing permissions and limitations
        under the License.

        SPDX-License-Identifier: Apache-2
 */
package io.aiven.kafka.connect.amqp.source;

import de.huxhorn.sulky.ulid.ULID;
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

public class AmqpSourceData
		implements
			NativeSourceData<ULID.Value, AmqpNativeInfo, AmqpOffsetManagerEntry, AmqpSourceRecord> {
	private final Receiver receiver;
	private final int streamLimit;

	AmqpSourceData(AmqpSourceConfig sourceConfig) throws ClientException {
		this.receiver = sourceConfig.getReceiver(sourceConfig.getConnection(sourceConfig.getClient()));
		streamLimit = 500; // sourceConfig.getStreamLimit();
	}

	@Override
	public String getSourceName() {
		return "AMQP Source";
	}

	@Override
	public Stream<AmqpNativeInfo> getNativeItemStream(ULID.Value ignore) {
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
	public IOSupplier<InputStream> getInputStream(AmqpSourceRecord amqpSourceRecord) {
		return () -> {
			try {
				return amqpSourceRecord.getNativeItem().getDelivery().rawInputStream();
			} catch (ClientException e) {
				throw new IOException(e);
			}
		};
	}

	@Override
	public ULID.Value getNativeKey(AmqpNativeInfo amqpNativeInfo) {
		return amqpNativeInfo.getNativeKey();
	}

	@Override
	public ULID.Value parseNativeKey(String keyString) {
		return ULID.parseULID(keyString);
	}

	@Override
	public AmqpSourceRecord createSourceRecord(AmqpNativeInfo amqpNativeInfo) {
		return new AmqpSourceRecord(amqpNativeInfo);
	}

	@Override
	public AmqpOffsetManagerEntry createOffsetManagerEntry(AmqpNativeInfo amqpNativeInfo) {
		return new AmqpOffsetManagerEntry(amqpNativeInfo.getNativeKey());
	}

	@Override
	public OffsetManager.OffsetManagerKey getOffsetManagerKey(ULID.Value nativeKey) {
		return new AmqpOffsetManagerEntry(nativeKey).getManagerKey();
	}

	@Override
	public Optional<Context<ULID.Value>> extractContext(AmqpNativeInfo nativeItem) {
		return Optional.of(new Context<ULID.Value>(nativeItem.getNativeKey()));
	}

}
