package com.sky.projects.kafka;

import com.sky.projects.kafka.producer.KafkaProducer;

public class KafkaProducerTest {

	public static void main(String[] args) {
		// kafka producer
		new Thread(new KafkaProducer("topic", "zt92:9092")).start();
	}
}
