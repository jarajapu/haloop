package org.apache.hadoop.mapred.iterative;

import org.apache.hadoop.mapred.JobConf;

public class DefaultLoopMapCacheSwitch implements LoopMapCacheSwitch {

	@Override
	public boolean isCacheRead(JobConf conf, int iteration, int step) {
		return false;
	}

	@Override
	public boolean isCacheWritten(JobConf conf, int iteration, int step) {
		return false;
	}

	@Override
	public Step getCacheStep(JobConf conf, int iteration, int step) {
		return new Step(0, 0);
	}

}
