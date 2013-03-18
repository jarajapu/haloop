package org.apache.hadoop.mapred.iterative;

import java.util.List;

import org.apache.hadoop.mapred.JobConf;

public interface LoopInputOutput {

	/**
	 * get loop input paths
	 * @param iteration
	 * @param step
	 * @return a list of string
	 */
	public List<String> getLoopInputs(JobConf conf, int iteration, int step);
	
	/**
	 * set loop output paths
	 * @param iteration
	 * @param step
	 * @return a list of string
	 */
	public String getLoopOutputs(JobConf conf, int iteration, int step);
	
}
