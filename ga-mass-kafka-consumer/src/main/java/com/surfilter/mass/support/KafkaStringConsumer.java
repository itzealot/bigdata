package com.surfilter.mass.support;

import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import com.surfilter.mass.MassContext;

/**
 * KafkaStringConsumer
 * 
 * @author zealot
 *
 */
public class KafkaStringConsumer implements Consumer<byte[]> {

	private BlockingQueue<String> queue;

	public KafkaStringConsumer(MassContext context) {
		this.queue = context.getStringQueue();
	}

	@Override
	public void accept(byte[] bytes) {
		try {
			queue.put(new String(bytes));
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
