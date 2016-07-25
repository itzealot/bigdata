package com.sky.projects.hadoop.common;

/**
 * AutoCloseable Util
 * 
 * @author zealot
 */
public final class Closeables {

	public static void close(AutoCloseable... closeables) {
		if (closeables != null) {
			for (AutoCloseable closeable : closeables)
				try {
					closeable.close();
				} catch (Exception e) {
				} finally {
					closeable = null;
				}
		}
	}

	private Closeables() {
	}
}
