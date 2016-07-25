package com.sky.projects.hadoop.mysql;

public class MysqlConfig {
	private String driverClassName;
	private String connectionUrl;
	private String userName;
	private String password;

	public MysqlConfig() {
		super();
	}

	public MysqlConfig(String driverClassName, String connectionUrl, String userName, String password) {
		super();
		this.driverClassName = driverClassName;
		this.connectionUrl = connectionUrl;
		this.userName = userName;
		this.password = password;
	}

	public String getDriverClassName() {
		return driverClassName;
	}

	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}

	public String getConnectionUrl() {
		return connectionUrl;
	}

	public void setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public String toString() {
		return "MysqlConfig [driverClassName=" + driverClassName + ", connectionUrl=" + connectionUrl + ", userName="
				+ userName + ", password=" + password + "]";
	}

}
