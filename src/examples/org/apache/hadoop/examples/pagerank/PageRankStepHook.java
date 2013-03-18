package org.apache.hadoop.examples.pagerank;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.iterative.LoopStepHook;

public class PageRankStepHook implements LoopStepHook {

	@Override
	public void preStep(JobConf conf, int iteration, int step) {
		System.out.println("start iteration " + iteration + " step " + step);
	}

	@Override
	public void postStep(JobConf conf, int iteration, int step) {
		System.out.println("finish iteration " + iteration + " step " + step);
	}

}
