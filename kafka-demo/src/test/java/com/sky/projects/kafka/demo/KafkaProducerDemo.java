package com.sky.projects.kafka.demo;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerDemo {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("metadata.broker.list", "rzx162:9092,rzx164:9092,rzx166:9092");
		// props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "-1");
		// props.put("partitioner.class", "");
		ProducerConfig config = new ProducerConfig(props);
		Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(config);
		final String topic = "topic-zt";

		for (int i = 0; i < 10; i++) {
			String message = "message " + i;
			System.out.println("send :" + message);
			producer.send(new KeyedMessage<byte[], byte[]>(topic, message.getBytes()));
		}
	}
}
