/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.module.loadgenerator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableModule;
import org.springframework.cloud.stream.annotation.Source;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

/**
 * A source that sends a set amount of empty byte array messages to verify the speed
 * of the infrastructure.
 *
 * @author Glenn Renfro
 */
@EnableModule(Source.class)
@EnableConfigurationProperties({LoadGeneratorSourceProperties.class})
public class LoadGeneratorSource extends AbstractEndpoint {

	private static final TestMessageHeaders HEADERS = new TestMessageHeaders(null);

	@Autowired
	private LoadGeneratorSourceProperties config;

	private final AtomicBoolean running = new AtomicBoolean(false);

	private ExecutorService executorService;

	Logger logger = LoggerFactory.getLogger(LoadGeneratorSource.class);


	@Autowired
	private Source channels;

	@Override
	protected void doStart() {
		executorService = Executors.newFixedThreadPool(config.getProducers());
		if (running.compareAndSet(false, true)) {
			for (int x = 0; x < config.getProducers(); x++) {
				executorService.execute(new Producer(x, channels));
			}
		}
	}
	@Override
	protected void doStop() {
		if (running.compareAndSet(true, false)) {
			executorService.shutdown();
		}
	}
	
	protected class Producer implements Runnable {

		int producerId;
		Source channels;

		public Producer(int producerId, Source channels) {
			this.producerId = producerId;
			this.channels = channels;
		}

		private void send() {
			logger.info("Producer " + producerId + " sending " + config.getMessageCount() + " messages");
			for (int x = 0; x < config.getMessageCount(); x++) {
				final byte[] message = createPayload(x);
				channels.output().send(new TestMessage(message));
			}
			logger.info("All Messages Dispatched");
		}

		/**
		 * Creates a message for consumption by the load-generator sink.  The payload will
		 * optionally contain a timestamp and sequence number if the generateTimestamp
		 * property is set to true.
		 *
		 * @param sequenceNumber a number to be prepended to the message
		 * @return a byte array containing a series of numbers that match the message size as
		 * specified by the messageSize constructor arg.
		 */
		private byte[] createPayload(int sequenceNumber) {
			byte message[] = new byte[config.getMessageSize()];
			if (config.isGenerateTimestamp()) {
				try {
					ByteArrayOutputStream acc = new ByteArrayOutputStream();
					DataOutputStream d = new DataOutputStream(acc);
					long nano = System.nanoTime();
					d.writeInt(sequenceNumber);
					d.writeLong(nano);
					d.flush();
					acc.flush();
					byte[] m = acc.toByteArray();
					if (m.length <= config.getMessageSize()) {
						System.arraycopy(m, 0, message, 0, m.length);
						return message;
					} else {
						return m;
					}
				} catch (IOException ioe) {
					throw new IllegalStateException(ioe);
				}
			} else {
				return message;
			}
		}

		public void run() {
			send();
		}

		private class TestMessage implements Message<byte[]> {
			private final byte[] message;

			private final TestMessageHeaders headers;

			public TestMessage(byte[] message) {
				this.message = message;
				this.headers = HEADERS;
			}

			@Override
			public byte[] getPayload() {
				return message;
			}

			@Override
			public MessageHeaders getHeaders() {
				return headers;
			}

		}

	}

	@SuppressWarnings("serial")
	private static class TestMessageHeaders extends MessageHeaders {
		public TestMessageHeaders(Map<String, Object> headers) {
			super(headers, ID_VALUE_NONE, -1L);
		}
	}
	
	
}
