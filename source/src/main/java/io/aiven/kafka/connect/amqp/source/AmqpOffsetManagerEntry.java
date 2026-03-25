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

        SPDX-License-Identifier: Apache-2
 */
package io.aiven.kafka.connect.amqp.source;

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.source.OffsetManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The OffsetManager Entry for the AMQP messages
 */
public final class AmqpOffsetManagerEntry implements OffsetManager.OffsetManagerEntry {
	private final ULID.Value primaryKey;
	private int recordCount;
	private final Map<String, Object> properties;

	private static final String PRIMARY_KEY = "ulid";
	private static final String RECORD_COUNT = "recordCount";
	private static final List<String> RESTRICTED = List.of(PRIMARY_KEY, RECORD_COUNT);

	AmqpOffsetManagerEntry(ULID.Value primaryKey) {
		Objects.requireNonNull(primaryKey, "primaryKey may not be null.");
		this.primaryKey = primaryKey;
		this.properties = new HashMap<>();
		this.recordCount = 0;
	}

	AmqpOffsetManagerEntry(Map<String, Object> props) {
		Object keyProp = props.get(PRIMARY_KEY);
		Objects.requireNonNull(keyProp, PRIMARY_KEY + " value not set.");
		primaryKey = keyProp instanceof ULID.Value ? (ULID.Value) keyProp : ULID.parseULID(keyProp.toString());
		Object recCount = props.get(RECORD_COUNT);
		if (recCount == null) {
			recordCount = 0;
		} else if (recCount instanceof String recStr) {
			recordCount = Integer.parseInt(recStr);
		} else if (recCount instanceof Number number) {
			recordCount = number.intValue();
		}
		properties = new HashMap<>(props);
	}

	@Override
	public AmqpOffsetManagerEntry fromProperties(Map<String, Object> properties) {
		Object keyProp = properties.get(PRIMARY_KEY);
		Objects.requireNonNull(keyProp, PRIMARY_KEY + " value not set.");
		ULID.Value key = keyProp instanceof ULID.Value ? (ULID.Value) keyProp : ULID.parseULID(keyProp.toString());
		return new AmqpOffsetManagerEntry(key);
	}

	@Override
	public Map<String, Object> getProperties() {
		Map<String, Object> result = new HashMap<>(properties);
		result.put(PRIMARY_KEY, primaryKey);
		result.put(RECORD_COUNT, recordCount);
		return result;
	}

	@Override
	public Object getProperty(String key) {
		if (PRIMARY_KEY.equals(key)) {
			return primaryKey;
		}
		if (RECORD_COUNT.equals(key)) {
			return recordCount;
		}
		return properties.get(key);
	}

	@Override
	public void setProperty(String key, Object value) {
		if (RESTRICTED.contains(key)) {
			throw new RuntimeException(key + " may not be set as a property");
		}
		properties.put(key, value);
	}

	@Override
	public OffsetManager.OffsetManagerKey getManagerKey() {
		return () -> Map.of(PRIMARY_KEY, primaryKey.toString(), RECORD_COUNT, recordCount);
	}

	@Override
	public void incrementRecordCount() {
		recordCount++;
	}

	@Override
	public long getRecordCount() {
		return recordCount;
	}
}
