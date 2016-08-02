package com.sky.projects.kafka.demo;

import com.sky.projects.kafka.thread.Threads;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class MessageConsumer implements Runnable {
	private KafkaStream<byte[], byte[]> stream;

	public MessageConsumer(KafkaStream<byte[], byte[]> stream) {
		this.stream = stream;
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = stream.iterator();

		while (it.hasNext()) {
			System.out.println("[" + Thread.currentThread().getName() + "] receive：" + new String(it.next().message()));
			Threads.sleep(1000);
		}
	}

}
