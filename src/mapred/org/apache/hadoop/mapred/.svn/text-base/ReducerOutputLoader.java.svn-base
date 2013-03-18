package org.apache.hadoop.mapred;

import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReducerOutputLoader {

	public static long getOutputSize(JobConf conf, String outputDir,
			int iteration) {
		try {
			Path fileFolder = new Path(outputDir, "i" + iteration + "/");
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] files = fs.listStatus(fileFolder);
			long size = 0;

			if (files == null)
				return size;

			for (int i = 0; i < files.length; i++)
				size += files[i].getLen();
			return size;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

	public static HashMap<String, String> loadReducerOutput(JobConf conf,
			String outputDir, int iteration) {
		HashMap<String, String> result = new HashMap<String, String>();
		// int previousIteration = iteration - 1;
		if (conf.isIterative() == false)
			return null;

		if (iteration < -1)
			return null;
		else {
			try {
				Path fileFolder = new Path(outputDir, "i" + iteration + "/");
				FileSystem fs = FileSystem.get(conf);
				FileStatus[] files = fs.listStatus(fileFolder);

				for (int i = 0; i < files.length; i++) {
					FSDataInputStream dis = fs.open(files[i].getPath());

					// for test purpose
					String line = null;

					while ((line = dis.readLine()) != null) {
						String[] keyvalue = line.split("\t");

						if (keyvalue.length >= 2)
							result.put(keyvalue[0], keyvalue[1]);
						else
							System.out
									.println("output result error in parsing to key and value");
					}
					dis.close();
				}

				// fs.close();
				return result;

			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}

		}
	}
}
