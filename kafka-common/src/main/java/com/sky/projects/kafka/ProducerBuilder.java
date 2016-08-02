package com.sky.projects.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerBuilder {

	public static Producer<String, String> build(String bootstrapServers) {
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		return new KafkaProducer<>(props);
	}

	public static void main(String[] args) {
		Producer<String, String> producer = build("rzx162:9092,rzx164:9092,rzx166:9092");
		for (int i = 0; i < 10; i++) {
			String message = "message " + i;
			System.out.println("send :" + message);
			producer.send(new ProducerRecord<String, String>("topic-zt", Integer.toString(i), message));
		}

		producer.close();
	}
}
