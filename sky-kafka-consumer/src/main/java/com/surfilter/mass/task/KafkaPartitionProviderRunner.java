package com.surfilter.mass.task;

import java.util.function.Consumer;

import com.surfilter.mass.MassRunnable;

import junit.framework.Assert;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * 消费某个topic的单个partition
 * 
 * @author zealot
 *
 */
public class KafkaPartitionProviderRunner implements MassRunnable {

	private KafkaStream<byte[], byte[]> kafkaStream;
	private Consumer<byte[]> consumer;

	public KafkaPartitionProviderRunner(KafkaStream<byte[], byte[]> kafkaStream, Consumer<byte[]> consumer) {
		this.kafkaStream = kafkaStream;
		Assert.assertNotNull("consumer can't be null", consumer);
		this.consumer = consumer;
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();

		while (it.hasNext()) {
			consumer.accept(it.next().message());
		}
	}

	@Override
	public void shutdown() {
		this.kafkaStream = null;
		this.consumer = null;
	}

}
