package com.sky.projects.kafka;

import com.sky.projects.kafka.consumer.KafkaConsumer;

/**
 * Kafka Consumer
 * 
 * @author zealot
 */
public class KafkaConsumerTest {

	public static void main(String[] args) {
		// kafka consumer
		new Thread(new KafkaConsumer("topic", "test-group", "zt92:2181")).start();
	}
}