package com.sky.projects.hadoop.common;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.Session;

/**
 * 
 * @author zealot
 */
public final class Ftps {

	public static Connection getConnection(String host, int port, String username, String password) throws Exception {
		Connection conn = new Connection(host, port);

		String message = "login error, where host: " + host + ", port: " + port + ", username: " + username
				+ ", password: " + password;
		try {
			conn.connect();
			if (!conn.authenticateWithPassword(username, password)) {
				throw new Exception(message);
			}
		} catch (Exception e) {
			throw new Exception(message, e);
		}

		return conn;
	}

	/**
	 * SFTP get connection by 22 port
	 * 
	 * @param host
	 * @param username
	 * @param password
	 * @return
	 * @throws Exception
	 */
	public static Connection getConnection(String host, String username, String password) throws Exception {
		return getConnection(host, 22, username, password);
	}

	public static SFTPv3Client exe(String host, String username, String password) throws Exception {
		return new SFTPv3Client(getConnection(host, username, password));
	}

	public static void exe(String host, String username, String password, String cmd) {
		Session session = null;
		Connection conn = null;

		try {
			conn = getConnection(host, username, password);
			session = conn.openSession();
			session.execCommand(cmd);
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		} finally {
			if (session != null) {
				session.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}

	private Ftps() {
	}
}
