package org.apache.hadoop.examples.pagerank;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.iterative.LoopReduceCacheSwitch;
import org.apache.hadoop.mapred.iterative.Step;

public class RankReduceCacheSwitch implements LoopReduceCacheSwitch {

	@Override
	public boolean isCacheWritten(JobConf conf, int iteration, int step) {
		if (step == 0 && iteration == 0)
			return true;
		else
			return false;
	}

	@Override
	public boolean isCacheRead(JobConf conf, int iteration, int step) {
		if (step == 0 && iteration > 0)
			return true;
		else
			return false;
	}
	
	@Override
	public Step getCacheStep(JobConf conf, int iteration, int step) {
		return new Step(0, 0);
	}
}
