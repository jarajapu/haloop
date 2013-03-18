package org.apache.hadoop.mapred.iterative;

import org.apache.hadoop.mapred.JobConf;

public interface LoopReduceOutputCacheSwitch {

	/**
	 * set up that in which iteration, cache is written
	 * @param iteration
	 * @param step
	 * @return a list of string
	 */
	boolean isCacheWritten(JobConf conf, int iteration, int step);

	/**
	 * set up that in which iteration, cache is read
	 * @param iteration
	 * @param step
	 * @return a list of string
	 */
	boolean isCacheRead(JobConf conf, int iteration, int step);
}
