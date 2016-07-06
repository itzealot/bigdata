package com.apusic.demo.consumer;

import com.apusic.demo.model.User;
import com.apusic.distribute.kafka.bus.KafkaMessageEventBus;
import com.apusic.distribute.message.bus.MessageEventConsumer;
import com.apusic.distribute.message.listener.MessageEventHandler;
import com.apusic.distribute.message.model.MessageEvent;

/**
 * 消息事件实体消费者，消费消息并对消息事件实体处理
 * 
 * @author zt
 */
public class MessageConsumer implements MessageEventHandler<User> {

	@Override
	public void handler(MessageEvent<User> event) {
		User usr = event.getMessage();
		System.out.println("Message: " + usr);
	}

	public static void main(String[] args) {
		MessageConsumer consumer = new MessageConsumer();
		String bootstrapServers = "172.20.129.154:9092,172.20.129.158:9092,172.20.129.159:9092";
		MessageEventConsumer bus = new KafkaMessageEventBus(bootstrapServers);

		// 根据 groupId 与 topic 消费消息
		bus.consumer("groupId-1", "event-1", consumer);
	}
}
