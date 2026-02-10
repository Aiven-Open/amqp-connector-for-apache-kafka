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
import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.AbstractSourceRecord;

public class AmqpSourceRecord
		extends
			AbstractSourceRecord<ULID.Value, AmqpNativeInfo, AmqpOffsetManagerEntry, AmqpSourceRecord> {
	private final AmqpOffsetManagerEntry offsetManagerEntry;

	public AmqpSourceRecord(AmqpNativeInfo amqpNativeInfo) {
		super(new NativeInfo<ULID.Value, AmqpNativeInfo>() {
			@Override
			public AmqpNativeInfo getNativeItem() {
				return amqpNativeInfo;
			}

			@Override
			public ULID.Value getNativeKey() {
				return amqpNativeInfo.getNativeKey();
			}

			@Override
			public long getNativeItemSize() {
				return NativeInfo.UNKNOWN_STREAM_LENGTH;
			}
		});
		this.offsetManagerEntry = new AmqpOffsetManagerEntry(amqpNativeInfo.getNativeKey());
	}

	@Override
	public AmqpSourceRecord duplicate() {
		return new AmqpSourceRecord(getNativeItem());
	}
}
