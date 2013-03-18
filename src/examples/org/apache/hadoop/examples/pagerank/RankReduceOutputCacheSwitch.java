package org.apache.hadoop.examples.pagerank;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.iterative.LoopReduceOutputCacheSwitch;

public class RankReduceOutputCacheSwitch implements LoopReduceOutputCacheSwitch {

	@Override
	public boolean isCacheWritten(JobConf conf, int iteration, int step) {
		if (step == 1)
			return true;
		else
			return false;
	}

	@Override
	public boolean isCacheRead(JobConf conf, int iteration, int step) {
		if (iteration > 0 && step == 1)
			return true;
		else
			return false;
	}

}
