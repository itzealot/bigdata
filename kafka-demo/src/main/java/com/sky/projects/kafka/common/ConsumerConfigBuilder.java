package com.sky.projects.kafka.common;

import java.util.Properties;

import kafka.consumer.ConsumerConfig;

/**
 * ConsumerConfig Builder
 * 
 * @author zt
 *
 */
public final class ConsumerConfigBuilder {

	/**
	 * 创建 ConsumerConfig 对象
	 * 
	 * @param groupId
	 * @param zkUrl
	 * @return
	 */
	public static ConsumerConfig createConsumerConfig(String groupId, String zkUrl) {
		Properties props = new Properties();

		props.put(KafkaConfig.GROUP_ID_PROPERTY, groupId);
		props.put(KafkaConfig.ZK_CONNECT_PROPERTY, zkUrl);
		props.put(KafkaConfig.ZK_SESSION_TIMEOUT_MS_PROPERTY, "400000");
		props.put(KafkaConfig.ZK_SYNC_TIME_MS_PROPERTY, "200");
		props.put("auto.offset.reset", "largest");
		props.put(KafkaConfig.AUTO_COMMIT_INTERVAL_MS_PROPERTY, "1000");

		return new ConsumerConfig(props);
	}

	private ConsumerConfigBuilder() {
	}
}
