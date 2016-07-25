package com.sky.projects.hadoop.hive;

public class TableDesc {
	private String filed;
	private String type;

	public TableDesc() {
		super();
	}

	public TableDesc(String filed, String type) {
		super();
		this.filed = filed;
		this.type = type;
	}

	@Override
	public String toString() {
		return "{filed=" + filed + ", type=" + type + "}";
	}

	public String getFiled() {
		return filed;
	}

	public void setFiled(String filed) {
		this.filed = filed;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}
