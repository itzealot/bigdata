package com.apusic.distribute.message.listener;

import com.apusic.distribute.message.model.MessageEvent;

import java.io.Serializable;

/**
 * 消息处理器
 * 
 * @author zt
 *
 * @param <T>
 */
public interface MessageEventHandler<T extends Serializable> {

	/**
	 * 处理消息事件实体 MessageEvent
	 * 
	 * @param event
	 *            消息
	 */
	void handler(MessageEvent<T> event);

}
