package com.sky.projects.kafka.demo;

import java.util.Properties;

import com.sky.projects.kafka.thread.Threads;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Kafka Producer demo
 * 
 * @author zealot
 */
public class KafkaProducerDemo {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("metadata.broker.list", "zt92:9092");
		props.put("request.required.acks", "-1");
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");

		ProducerConfig config = new ProducerConfig(props);
		Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(config);
		final String topic = "test";

		int i = 0;
		while (true) {
			String message = "message " + i;
			System.out.println("send :" + message);
			producer.send(new KeyedMessage<byte[], byte[]>(topic, message.getBytes()));
			Threads.sleep(1000);
			i++;
		}
	}
}
