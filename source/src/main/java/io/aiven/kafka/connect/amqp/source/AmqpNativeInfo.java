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
import org.apache.qpid.protonj2.client.Delivery;

/**
 * Wraps ULID.Value and the Delivery for a streaming object. The framework
 * requires a NativeInfo that has both the key and the value associated with
 * that key. This implementation creates a ULID value for every Delivery from
 * AMQP.
 */
public class AmqpNativeInfo implements Comparable<AmqpNativeInfo> {
	/** The ULID to generate keys with */
	private static final ULID ulid = new ULID();
	/** The AMQP Delivery object */
	private final Delivery delivery;
	/** THe ULID value for the key */
	private final ULID.Value value;

	/**
	 * Construct native info for a Delivery from AMQP.
	 * 
	 * @param delivery
	 *            the delivery to process.
	 */
	public AmqpNativeInfo(Delivery delivery) {
		this.delivery = delivery;
		this.value = ulid.nextValue();
	}

	@Override
	public int compareTo(AmqpNativeInfo other) {
		return value.compareTo(other.value);
	}

	/**
	 * Gets the AMQP Delivery object.
	 * 
	 * @return the AMQP delivery.
	 */
	public Delivery getDelivery() {
		return delivery;
	}

	/**
	 * Gets the generated key.
	 * 
	 * @return the generated key.
	 */
	public ULID.Value getNativeKey() {
		return value;
	}
}
