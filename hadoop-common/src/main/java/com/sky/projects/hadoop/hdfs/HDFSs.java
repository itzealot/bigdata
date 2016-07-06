package com.sky.projects.hadoop.hdfs;

import static com.sky.projects.hadoop.common.Closeables.close;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sky.projects.hadoop.common.HadoopConfigurationBuilder;

public final class HDFSs {
	private static final Logger LOG = LoggerFactory.getLogger(HDFSs.class);
	public static final int BUFFER_SIZE_10X = 1000 * 10;

	private static String hdfs = "";
	private static Configuration conf = HadoopConfigurationBuilder.create(hdfs);
	private static FileSystem fs = null;

	{
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		}
	}

	public static boolean mkdirs(String path) {
		return FileSystems.mkdirs(fs, path);
	}

	public static void copy2LocalFile(String src, String dest) {
		FileSystems.copy2LocalFile(fs, src, dest);
	}

	public static boolean delete(String path) {
		return FileSystems.deleteOnExit(fs, path);
	}

	/**
	 * 遍历HDFS上的文件和目录
	 * 
	 * @return
	 */
	public static List<String> dirs() {
		return FileSystems.dirs(fs);
	}

	public static boolean move(String src, String local) {
		return FileSystems.moveToLocalFile(fs, src, local);
	}

	public static boolean copy2Hdfs(String src, String hdfs) {
		return FileSystems.copy2Hdfs(fs, src, hdfs);
	}

	/**
	 * 将HDFS上文件重新命名
	 * 
	 * @param conf
	 * @param src
	 * @param dst
	 * @return
	 * @throws IOException
	 */
	public static boolean renameFrom(Configuration conf, String src, String dst) throws IOException {
		boolean returnFlag = false;
		FileSystem fs = null;

		try {
			fs = FileSystem.get(conf);

			Path srcPath = new Path(src);
			Path dstPath = new Path(dst);

			if (fs.exists(srcPath)) {
				if (fs.exists(dstPath)) {
					fs.delete(dstPath, true);
				} else if (!fs.exists(dstPath.getParent())) {
					fs.mkdirs(dstPath.getParent());
				}

				returnFlag = fs.rename(srcPath, dstPath);
			} else {
				returnFlag = true;
			}
		} catch (Exception e) {
			// TODO
		} finally {
			close(fs);
		}

		return returnFlag;
	}

	/**
	 * 文件检测并删除
	 * 
	 * @param path
	 * @param conf
	 * @return
	 */
	public static boolean checkAndDel(final String path, Configuration conf) {
		Path dstPath = new Path(path);
		FileSystem hdfs = null;

		try {
			hdfs = dstPath.getFileSystem(conf);
			if (hdfs.exists(dstPath)) {
				hdfs.delete(dstPath, true);
			} else {
				return true;
			}
		} catch (IOException e) {
			LOG.error("checkAndDel error: " + e.getMessage());
			return false;
		} finally {
			close(hdfs);
		}

		return true;
	}

	public static void setReplication(String hadoopFs, String inputPath, short repNum) {
		String hadoopOwner = "anonymous";
		System.setProperty("HADOOP_USER_NAME", hadoopOwner);
		Configuration conf = new Configuration();

		conf.set("fs.defaultFS", hadoopFs);
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);

			Path path = new Path(inputPath);

			if (fs.exists(path)) {
				FileStatus[] fileStatus = fs.listStatus(path);
				if (fileStatus != null) {
					for (FileStatus eleFs : fileStatus) {
						fs.setReplication(eleFs.getPath(), repNum);
					}
				}
			}
		} catch (Exception e) {
			LOG.error("setReplication error , inputPath is " + inputPath, e);
		} finally {
			close(fs);
		}
	}

}
