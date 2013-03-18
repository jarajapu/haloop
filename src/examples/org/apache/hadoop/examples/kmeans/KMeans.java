package org.apache.hadoop.examples.kmeans;

import org.apache.hadoop.examples.kmeans.NaiveKMeans.KMeansMapper;
import org.apache.hadoop.examples.kmeans.NaiveKMeans.KMeansReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class KMeans {

	public static void main(String[] args) throws Exception {

		String inputPath = args[0];
		String outputPath = args[1];

		int specIteration = 0;
		if (args.length > 2) {
			specIteration = Integer.parseInt(args[2]);
		}

		int numReducers = 3;
		if (args.length > 3) {
			numReducers = Integer.parseInt(args[3]);
		}

		long start = System.currentTimeMillis();
		// step
		
		/**
		 * kmeans loop body map/reduce step
		 */
		JobConf conf = new JobConf(KMeans.class);
		conf.setMapperClass(KMeansMapper.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setCombinerClass(KMeansReducer.class);
		conf.setReducerClass(KMeansReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// as-a-while iterative job
		JobConf job = new JobConf(KMeans.class);
		job.setNumIterations(specIteration);
		job.setNumReduceTasks(numReducers);
		job.setSpeculativeExecution(false);
		job.setInputPath(inputPath);
		job.setOutputPath(outputPath);
		job.setStepConf(0, conf);
		job.setIterative(true);
		job.setMapperInputCacheOption(true);
		job.setLoopMapCacheFilter(KMeansLoopMapCacheFilter.class);
		job.setLoopMapCacheSwitch(KMeansLoopMapCacheSwitch.class);
		job.setLoopInputOutput(KMeansLoopInputOutput.class);
		job.setJobName("iterative k-means");

		// run the job
		JobClient.runJob(job);
		long end = System.currentTimeMillis();
		System.out.println("running time " + (end - start) / 1000 + "s");
	}
}