package com.sky.projects.kafka.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumerDemo {

	private static final String ZK_URL = "rzx162:2181,rzx164:2181,rzx166:2181/kafka";
	private static final String TOPIC = "topic-zt";

	public static void main(String[] args) {
		ConsumerConnector connector = Consumer.createJavaConsumerConnector(create());

		Map<String, Integer> topicsMap = new HashMap<String, Integer>();
		topicsMap.put(TOPIC, 3);
		List<KafkaStream<byte[], byte[]>> partitions = connector.createMessageStreams(topicsMap).get(TOPIC);

		for (KafkaStream<byte[], byte[]> partition : partitions) {
			new Thread(new MessageConsumer(partition)).start();
		}
	}

	public static ConsumerConfig create() {
		Properties properties = new Properties();

		properties.setProperty("zookeeper.connect", ZK_URL);
		properties.setProperty("zookeeper.connection.timeout.ms", "100000");
		properties.setProperty("auto.offset.reset", "largest");
		properties.setProperty("group.id", "test");

		return new ConsumerConfig(properties);
	}

}
