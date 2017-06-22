package com.surfilter.mass.provider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import com.surfilter.mass.MassConsts;
import com.surfilter.mass.MassContext;
import com.surfilter.mass.task.KafkaPartitionProviderRunner;

import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Kafka 数据生产者，针对一类数据(指定topicName与partitions)从Kafka拉取数据
 * 
 * @author zealot
 *
 */
public class KafkaProvider implements Provider {

	private ConsumerConnector connector;
	private ExecutorService threadPool;
	private TopicAndPartition topicAndPartition;
	private Consumer<byte[]> consumer;

	public KafkaProvider(MassContext context, TopicAndPartition topicAndPartition, Consumer<byte[]> consumer) {
		this.connector = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(initKafkaProps(context)));
		this.topicAndPartition = topicAndPartition;
		this.consumer = consumer;
	}

	private Properties initKafkaProps(MassContext context) {
		Properties properties = new Properties();

		properties.setProperty("zookeeper.connect", context.getStrings(MassConsts.KAFKA_ZK_URL));
		properties.setProperty("zookeeper.connection.timeout.ms", "100000");
		properties.setProperty("auto.offset.reset", "largest");
		properties.setProperty("group.id", context.get(MassConsts.KAFKA_GROUP_ID));

		/*
		 * 解决 ConsumerRebalanceFailedException, can't rebalance after {num}
		 * retries 问题的测试性参数
		 */
		properties.setProperty("zookeeper.session.timeout.ms", "5000");
		properties.setProperty("rebalance.max.retries", "10");
		properties.setProperty("rebalance.backoff.ms", "2000");

		return properties;
	}

	@Override
	public void provide() {
		Map<String, Integer> topicsMap = new HashMap<String, Integer>(2);
		String topic = topicAndPartition.topic();
		int partition = topicAndPartition.partition();
		topicsMap.put(topic, partition);

		List<KafkaStream<byte[], byte[]>> kafkaStreams = connector.createMessageStreams(topicsMap).get(topic);

		// new fixed Thread Pool
		threadPool = Executors.newFixedThreadPool(partition);

		// for each every partition submitting Task
		for (KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams) {
			threadPool.execute(new KafkaPartitionProviderRunner(kafkaStream, consumer));
		}
	}

	@Override
	public void close() {
		try {
			threadPool.shutdownNow();
		} catch (Exception e) {
			Thread.currentThread().interrupt();
		} finally {
			connector.shutdown();
		}
	}

}
