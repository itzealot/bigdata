package com.sky.projects.kafka.thread;

public final class Threads {

	public static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private Threads() {
	}
}