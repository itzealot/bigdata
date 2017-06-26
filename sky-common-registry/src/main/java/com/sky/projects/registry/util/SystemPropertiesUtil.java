package com.sky.projects.registry.util;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 关于系统Properties的工具类
 * 
 * 1. 统一的读取系统变量，其中Boolean.readBoolean的风格不统一，Double则不支持，都进行了扩展.
 * 
 * 2. 简单的合并系统变量(-D)，环境变量 和默认值，以系统变量优先，在未引入Commons Config时使用.
 * 
 * 3. Properties
 * 本质上是一个HashTable，每次读写都会加锁，所以不支持频繁的System.getProperty(name)来检查系统内容变化
 * 因此扩展了一个ListenableProperties, 在其所关心的属性变化时进行通知.
 * 
 * @author calvin
 */
public class SystemPropertiesUtil {

	/**
	 * 读取String类型的系统变量，为空时返回null.
	 */
	public static String getString(String name) {
		return System.getProperty(name);
	}

	/**
	 * 读取String类型的系统变量，为空时返回默认值
	 */
	public static String getString(String name, String defaultValue) {
		return System.getProperty(name, defaultValue);
	}

	/**
	 * 读取Integer类型的系统变量，为空时返回null.
	 */
	public static Integer getInteger(String name) {
		return Integer.getInteger(name);
	}

	/**
	 * 读取Integer类型的系统变量，为空时返回默认值
	 */
	public static Integer getInteger(String name, Integer defaultValue) {
		return Integer.getInteger(name, defaultValue);
	}

	/**
	 * 读取Long类型的系统变量，为空时返回null.
	 */
	public static Long getLong(String name) {
		return Long.getLong(name);
	}

	/**
	 * 读取Integer类型的系统变量，为空时返回默认值
	 */
	public static Long getLong(String name, Long defaultValue) {
		return Long.getLong(name, defaultValue);
	}

	/////////// 简单的合并系统变量(-D)，环境变量 和默认值，以系统变量优先.///////////////

	/**
	 * 合并系统变量(-D)，环境变量 和默认值，以系统变量优先
	 */
	public static String getString(String propertyName, String envName, String defaultValue) {
		checkEnvName(envName);
		String propertyValue = System.getProperty(propertyName);
		if (propertyValue != null) {
			return propertyValue;
		} else {
			propertyValue = System.getenv(envName);
			return propertyValue != null ? propertyValue : defaultValue;
		}
	}

	/////////// ListenableProperties /////////////
	/**
	 * Properties
	 * 本质上是一个HashTable，每次读写都会加锁，所以不支持频繁的System.getProperty(name)来检查系统内容变化
	 * 因此扩展了一个ListenableProperties, 在其所关心的属性变化时进行通知.
	 * 
	 * @see ListenableProperties
	 */
	public static synchronized void registerSystemPropertiesListener(PropertiesListener listener) {
		Properties currentProperties = System.getProperties();

		// 不能进行实例化，则构建 ListenableProperties
		if (!(currentProperties instanceof ListenableProperties)) {
			ListenableProperties newProperties = new ListenableProperties(currentProperties);
			System.setProperties(newProperties);
			currentProperties = newProperties;
		}

		((ListenableProperties) currentProperties).register(listener);
	}

	/**
	 * 检查环境变量名不能有'.'，在linux下不支持
	 */
	private static void checkEnvName(String envName) {
		if (envName == null || envName.indexOf('.') != -1) {
			throw new IllegalArgumentException("envName " + envName + " has dot which is not valid");
		}
	}

	/**
	 * Properties
	 * 本质上是一个HashTable，每次读写都会加锁，所以不支持频繁的System.getProperty(name)来检查系统内容变化
	 * 因此扩展了一个ListenableProperties, 在其所关心的属性变化时进行通知.
	 * 
	 * @see PropertiesListener
	 */
	public static class ListenableProperties extends Properties {

		private static final long serialVersionUID = -8282465702074684324L;

		/** all listeners */
		protected List<PropertiesListener> listeners = new CopyOnWriteArrayList<PropertiesListener>();

		public ListenableProperties(Properties properties) {
			super(properties);
		}

		/**
		 * 注册监听器
		 * 
		 * @param listener
		 */
		public void register(PropertiesListener listener) {
			listeners.add(listener);
		}

		@Override
		public synchronized Object setProperty(String key, String value) {
			Object result = put(key, value);

			// 内容改变时通知所有监听器
			for (PropertiesListener listener : listeners) {
				if (listener.propertyName.equals(key)) {
					listener.onChange(key, value);
				}
			}

			return result;
		}
	}

	/**
	 * 获取所关心的Properties变更的Listener基类.
	 */
	public abstract static class PropertiesListener {
		protected String propertyName;

		public PropertiesListener(String propertyName) {
			this.propertyName = propertyName;
		}

		/**
		 * 监听改变时事件
		 * 
		 * @param propertyName
		 * @param value
		 */
		public abstract void onChange(String propertyName, String value);
	}
}
