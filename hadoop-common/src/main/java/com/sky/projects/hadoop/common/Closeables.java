package com.sky.projects.hadoop.common;

public final class Closeables {

	public static void close(AutoCloseable... closeables) {
		if (closeables != null) {
			for (AutoCloseable closeable : closeables)
				try {
					closeable.close();
				} catch (Exception e) {
					// TODO
					e.printStackTrace();
				} finally {
					closeable = null;
				}
		}
	}

	private Closeables() {
	}
}
