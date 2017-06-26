package com.sky.projects.registry.entity;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 注册到节点的信息
 * 
 * @author zealot
 *
 */
public class RegistryInfo {

	private static final AtomicInteger AT = new AtomicInteger(1);

	/** 组件ID */
	private Integer appId;
	/** 组件名称 */
	private String registerName;
	/** IP地址 */
	private String ipAddr;
	/** 机器名 */
	private String hostName;
	/** 启动时间，绝对秒数 */
	private Long startTime;
	/** 程序目录 */
	private String appDir;
	/** 组件版本信息 */
	private String version;

	public RegistryInfo() {
		super();
	}

	public RegistryInfo(String registerName, String ipAddr, String hostName, Long startTime, String appDir,
			String version) {
		super();
		this.appId = AT.getAndIncrement();
		this.registerName = registerName;
		this.ipAddr = ipAddr;
		this.hostName = hostName;
		this.startTime = startTime;
		this.appDir = appDir;
		this.version = version;
	}

	public Integer getAppId() {
		return appId;
	}

	public void setAppId(Integer appId) {
		this.appId = appId;
	}

	public String getRegisterName() {
		return registerName;
	}

	public void setRegisterName(String registerName) {
		this.registerName = registerName;
	}

	public String getIpAddr() {
		return ipAddr;
	}

	public void setIpAddr(String ipAddr) {
		this.ipAddr = ipAddr;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public String getAppDir() {
		return appDir;
	}

	public void setAppDir(String appDir) {
		this.appDir = appDir;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

}
