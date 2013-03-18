package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

public class FileCache {

	private FileSystem localFs = null;// FileSystem.get(new Configuration());

	private Path path = null;

	private Text value = null;

	// writing
	private FSDataOutputStream dos = null;// hdfs.create(path);

	private FSDataInputStream dis = null;

	public FileCache(String fileName) {
		try {
			localFs = FileSystem.getLocal(new Configuration());
			path = new Path(fileName);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void openForRead() throws IOException {
		dis = localFs.open(path);
	}

	public void openForWrite() throws IOException {
		dos = localFs.create(path);
	}

	/**
	 * put the tuple into the file
	 */
	public void put(Text value) throws IOException {
		value.write(dos);
	}

	public boolean hasNext() throws IOException {
		// TODO Auto-generated method stub
		return dis.available() > 0;
	}

	public Text next() throws IOException {
		// TODO Auto-generated method stub
		// value = ReflectionUtils.newInstance(valueClass, conf);
		value = new Text();
		value.readFields(dis);
		return value;
	}

	public void removeFile() throws IOException {
		// TODO Auto-generated method stub
		localFs.delete(path);
	}

	public void reWind() throws IOException {
		dis.seek(0);
	}

	public void closeRead() throws IOException {
		dis.close();
	}

	public void closeWrite() throws IOException {
		dos.flush();
		dos.close();
	}
}
