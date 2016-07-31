package com.sky.projects.kafka.common;

import java.util.Properties;

import kafka.producer.ProducerConfig;

/**
 * ProducerConfig Builder
 * 
 * @author zt
 *
 */
public final class ProducerConfigBuilder {

	/**
	 * 创建 ProducerConfig 对象
	 * 
	 * @return
	 */
	public static ProducerConfig createProducerConfig() {
		return createProducerConfig(KafkaConfig.DEFAULT_METADAT_BROKER_LIST);
	}

	/**
	 * 创建 ProducerConfig 对象
	 * 
	 * @return
	 */
	public static ProducerConfig createProducerConfig(String brokers) {
		Properties props = new Properties();

		props.put(KafkaConfig.METADAT_BROKER_LIST_PROPERTY, brokers);
		props.put(KafkaConfig.SERIALIZER_CLASS_PROPERTY, KafkaConfig.DEFAULT_SERIALIZER_CLASS);

		return new ProducerConfig(props);
	}

	private ProducerConfigBuilder() {
	}
}
