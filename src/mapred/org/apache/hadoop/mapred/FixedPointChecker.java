package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.iterative.IFixPointChecker;

public class FixedPointChecker implements IFixPointChecker {

	/**
	 * job configuration
	 */
	private JobConf conf = null;

	/**
	 * the iterative job's JobID
	 */
	private JobID jobId;

	/**
	 * aggregate distance calculated by each reducer
	 * 
	 * @param conf
	 * @return
	 */
	private float aggregateDistance(JobConf conf) {
		float distance = 0f;
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(MRConstants.SCHEDULE_LOG_DIR + "/" + jobId
					+ "/distance/");
			FileStatus[] files = fs.listStatus(path);

			for (FileStatus file : files) {
				FSDataInputStream input = fs.open(file.getPath());
				float dist = input.readFloat();
				distance += dist;
				input.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("distance: " + distance);
		return distance;
	}

	/**
	 * the fixed point checker constructor
	 * 
	 * @param c
	 * @param iteration
	 */
	public FixedPointChecker() {

	}

	/**
	 * Yingyi: add the logic to determine whether fixed point is achieved
	 * 
	 * @return
	 */
	public boolean IsFixedPoint(JobConf job, JobID id, int iteration, int step) {
		if (iteration == 0)
			return false;
		this.conf = job;
		this.jobId = id;
		float dist = this.aggregateDistance(conf);
		if (dist <= conf.getDistanceThreshold())
			return true;
		else
			return false;
	}

}
