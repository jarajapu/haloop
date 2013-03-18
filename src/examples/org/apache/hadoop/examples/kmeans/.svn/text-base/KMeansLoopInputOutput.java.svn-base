package org.apache.hadoop.examples.kmeans;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.iterative.LoopInputOutput;

public class KMeansLoopInputOutput implements LoopInputOutput {

	@Override
	public List<String> getLoopInputs(JobConf conf, int iteration, int step) {
		List<String> paths = new ArrayList<String>();
		paths.add(conf.getInputPath());
		return paths;
	}

	@Override
	public String getLoopOutputs(JobConf conf, int iteration, int step) {
		return (conf.getOutputPath() + "/i" + iteration);
	}

}
