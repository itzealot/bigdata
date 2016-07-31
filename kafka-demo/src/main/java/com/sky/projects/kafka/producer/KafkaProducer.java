package com.sky.projects.kafka.producer;

import com.sky.projects.kafka.common.ProducerConfigBuilder;
import com.sky.projects.kafka.thread.Threads;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * Kafka 消息生产者
 * 
 * @author zt
 *
 */
public class KafkaProducer implements Runnable {
	// kafka producer
	private final Producer<Integer, String> producer;
	private int messageNo = 1;;
	private final String topic;

	public KafkaProducer(String topic, String brokers) {
		this.topic = topic;
		producer = new Producer<Integer, String>(ProducerConfigBuilder.createProducerConfig(brokers));
	}

	@Override
	public void run() {
		while (true) {
			String messageStr = new String("Message_" + messageNo++);
			System.out.println("Send:" + messageStr);
			producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
			Threads.sleep(2000);
		}
	}
}