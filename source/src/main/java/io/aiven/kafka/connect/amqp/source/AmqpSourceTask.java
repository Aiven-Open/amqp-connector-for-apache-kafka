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

import io.aiven.commons.kafka.connector.source.AbstractSourceRecordIterator;
import io.aiven.commons.kafka.connector.source.AbstractSourceTask;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.transformer.Transformer;
import io.aiven.commons.timing.BackoffConfig;
import io.aiven.commons.version.VersionInfo;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class AmqpSourceTask extends AbstractSourceTask {
	/** The logger to write to */
	private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSourceTask.class);

	private AmqpSourceConfig amqpSourceConfig;
	private Transformer transformer;
	private OffsetManager<AmqpOffsetManagerEntry> offsetManager;
	private AmqpSourceData amqpSourceData;

	private AbstractSourceRecordIterator<ULID.Value, AmqpNativeInfo, AmqpOffsetManagerEntry, AmqpSourceRecord> sourceRecordIterator;
	@Override
	protected Iterator<SourceRecord> getIterator(BackoffConfig config) {
		return IteratorUtils.transformedIterator(sourceRecordIterator, amqpSourceRecord -> amqpSourceRecord
				.getSourceRecord(amqpSourceConfig.getErrorsTolerance(), offsetManager));
	}

	@Override
	protected AmqpSourceConfig configure(Map<String, String> props) {
		LOGGER.info("AMQP Source task started.");
		this.amqpSourceConfig = new AmqpSourceConfig(props);
		this.transformer = amqpSourceConfig.getTransformer();
		this.offsetManager = new OffsetManager<>(context);
		try {
			this.amqpSourceData = new AmqpSourceData(amqpSourceConfig);
		} catch (ClientException e) {
			throw new RuntimeException(e);
		}
		setSourceRecordIterator(
				new AbstractSourceRecordIterator<ULID.Value, AmqpNativeInfo, AmqpOffsetManagerEntry, AmqpSourceRecord>(
						amqpSourceConfig, transformer, offsetManager, amqpSourceData));

		return amqpSourceConfig;
	}

	/**
	 * Used in testing.
	 * 
	 * @param iterator
	 *            the iterator to use.
	 */
	void setSourceRecordIterator(
			AbstractSourceRecordIterator<ULID.Value, AmqpNativeInfo, AmqpOffsetManagerEntry, AmqpSourceRecord> iterator) {
		this.sourceRecordIterator = iterator;
	}

	@Override
	protected void closeResources() {

	}

	@Override
	public String version() {
		return new VersionInfo(this.getClass()).getVersion();
	}
}
