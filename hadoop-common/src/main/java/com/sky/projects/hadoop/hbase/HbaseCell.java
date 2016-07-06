package com.sky.projects.hadoop.hbase;

public class HbaseCell {
	private String rowKey;
	private String columnFamily;
	private String qualifier;
	private String value;
	private long timestamp;

	public HbaseCell() {
		super();
	}

	public HbaseCell(String rowKey, String columnFamily, String qualifier, String value, long timestamp) {
		super();
		this.rowKey = rowKey;
		this.columnFamily = columnFamily;
		this.qualifier = qualifier;
		this.value = value;
		this.timestamp = timestamp;
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getQualifier() {
		return qualifier;
	}

	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "HbaseResult [rowKey=" + rowKey + ", columnFamily=" + columnFamily + ", qualifier=" + qualifier
				+ ", value=" + value + ", timestamp=" + timestamp + "]";
	}

}
