package org.apache.hadoop.mapred.iterative;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

public class DefaultLoopInputOutput implements LoopInputOutput {

	@Override
	public List<String> getLoopInputs(JobConf conf, int iteration, int step) {
		Path[] paths = FileInputFormat.getInputPaths(conf);
		List<String> inputPaths = new ArrayList<String>();
		for (Path p : paths)
			inputPaths.add(p.toString());
		return inputPaths;
	}

	@Override
	public String getLoopOutputs(JobConf conf, int iteration, int step) {
		return FileOutputFormat.getOutputPath(conf).toString();
	}

}
