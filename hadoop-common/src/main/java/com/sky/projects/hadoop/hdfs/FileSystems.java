package com.sky.projects.hadoop.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class FileSystems {

	/**
	 * true if deleteOnExit is successful, otherwise false.
	 * 
	 * @param fs
	 * @param path
	 * @return
	 */
	public static boolean deleteOnExit(FileSystem fs, final String path) {
		try {
			return fs.deleteOnExit(new Path(path));
		} catch (IOException e) {
			return false;
		}
	}

	/**
	 * Check if exists, true is exists , otherwise is false.
	 * 
	 * @param fs
	 * @param path
	 * @return
	 */
	public static boolean exists(FileSystem fs, final String path) {
		try {
			return fs.exists(new Path(path));
		} catch (IOException e) {
			return false;
		}
	}

	/**
	 * true is if mkdirs is successful, otherwise false.
	 * 
	 * @param fs
	 * @param path
	 * @return
	 */
	public static boolean mkdirs(FileSystem fs, final String path) {
		try {
			return fs.mkdirs(new Path(path));
		} catch (IOException e) {
			return false;
		}
	}

	/**
	 * The src file is under FS, and the dst is on the local disk. Copy it from
	 * FS control to the local dst name.
	 * 
	 * @param fs
	 * @param hdfs
	 * @param dest
	 */
	public static boolean copy2LocalFile(FileSystem fs, final String hdfs, final String dest) {
		try {
			fs.copyToLocalFile(new Path(hdfs), new Path(dest));
			return true;
		} catch (IOException e) {
			// TODO
			return false;
		}
	}

	public static List<String> dirs(FileSystem fs) {
		List<String> lists = new ArrayList<String>();

		try {
			FileStatus status[] = fs.listStatus(new Path(fs.getUri()));

			for (int i = 0, size = status.length; i < size; i++) {
				lists.add(status[i].getPath().getName());
			}
		} catch (IOException e) {
			// TODO
		}

		return lists;
	}

	public static boolean moveToLocalFile(FileSystem fs, final String hdfs, final String dest) {
		try {
			fs.moveToLocalFile(new Path(hdfs), new Path(dest));
			return true;
		} catch (IOException e) {
			// TODO
			return false;
		}
	}

	/**
	 * The src file is on the local disk. Add it to FS at the given dst name and
	 * the source is kept intact afterwards
	 * 
	 * @param fs
	 * @param src
	 * @param hdfs
	 * @return
	 */
	public static boolean copy2Hdfs(FileSystem fs, final String src, final String hdfs) {
		try {
			fs.copyFromLocalFile(new Path(src), new Path(hdfs));
			return true;
		} catch (IOException e) {
			// TODO
			return false;
		}
	}

	private FileSystems() {
	}
}
