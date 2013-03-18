package org.apache.hadoop.examples.descendant;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.iterative.LoopInputOutput;

public class DescendantLoopInputOutput implements LoopInputOutput {

	@Override
	public List<String> getLoopInputs(JobConf conf, int iteration, int step) {
		List<String> paths = new ArrayList<String>();
		int currentPass = 2 * iteration + step;

		if (currentPass > 0) {
			if (step == 0)
				/**
				 * for the join step
				 */
				paths.add(conf.getOutputPath() + "/i" + (currentPass - 1));
			if (step == 1) {
				/**
				 * for the duplicate elimination step
				 */
				for (int i = 1; i < currentPass; i += 2)
					paths.add(conf.getOutputPath() + "/i" + i);
				paths.add(conf.getOutputPath() + "/i" + (currentPass - 1));
			}
		}

		if (currentPass == 0)
			/**
			 * only initial step reads the graph
			 */
			paths.add(conf.getInputPath());

		return paths;
	}

	@Override
	public String getLoopOutputs(JobConf conf, int iteration, int step) {
		int currentPass = 2 * iteration + step;
		return (conf.getOutputPath() + "/i" + currentPass);
	}

}