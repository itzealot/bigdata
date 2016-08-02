package com.sky.projects.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sky.projects.kafka.common.ConsumerConfigBuilder;
import com.sky.projects.kafka.thread.Threads;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Kafka 消息消费者
 * 
 * @author zt
 *
 */
public class KafkaConsumer implements Runnable {
	private final ConsumerConnector consumer;
	private final String topic;

	public KafkaConsumer(String topic, String groupId, String zkUrl) {
		this.topic = topic;
		consumer = Consumer.createJavaConsumerConnector(ConsumerConfigBuilder.createConsumerConfig(groupId, zkUrl));
	}

	@Override
	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		// topic and partition count
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

		// get partition[0]
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();

		while (it.hasNext()) {
			System.out.println("receive：" + new String(it.next().message()));
			Threads.sleep(1000);
		}
	}
}