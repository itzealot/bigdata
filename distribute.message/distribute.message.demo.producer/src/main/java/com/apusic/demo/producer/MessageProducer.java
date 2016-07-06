package com.apusic.demo.producer;

import com.apusic.demo.model.Role;
import com.apusic.demo.model.User;
import com.apusic.distribute.kafka.publisher.KafkaMessageEventPublisher;
import com.apusic.distribute.message.model.MessageEvent;
import com.apusic.distribute.message.publisher.MessageEventPublisher;

/**
 * 消息生产者
 * 
 * @author zt
 */
public class MessageProducer {

	public static void main(String[] args) {
		String bootstrapServers = "172.20.129.154:9092,172.20.129.158:9092,172.20.129.159:9092";
		MessageEventPublisher<User> pub = new KafkaMessageEventPublisher<User>(bootstrapServers);

		// 创建消息实体 MessageEvent
		MessageEvent<User> me = new MessageEvent<User>();
		// 消息实体 ,topic is: event-1
		me.setEventType("event-1");

		// 以下为发送的消息体
		Role role = new Role();
		role.setId(1);
		role.setName("Admin");
		role.setDescription("Super administrator");

		User user = new User();
		user.setRole(role);
		user.setName("Scott");
		user.setPassowrd("thefox");
		user.setStatus("enable");

		me.setMessage(user);

		// 发送消息
		pub.publish(me);
	}
}
