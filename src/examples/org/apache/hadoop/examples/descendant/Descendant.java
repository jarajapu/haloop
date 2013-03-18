package org.apache.hadoop.examples.descendant;

import org.apache.hadoop.examples.descendant.NaiveDescendant.AggregateResultMap;
import org.apache.hadoop.examples.descendant.NaiveDescendant.AggregateResultReduce;
import org.apache.hadoop.examples.descendant.NaiveDescendant.DuplicateEliminateMap;
import org.apache.hadoop.examples.descendant.NaiveDescendant.DuplicateEliminateReduce;
import org.apache.hadoop.examples.descendant.NaiveDescendant.JoinMap;
import org.apache.hadoop.examples.descendant.NaiveDescendant.JoinReduce;
import org.apache.hadoop.examples.textpair.FirstPartitioner;
import org.apache.hadoop.examples.textpair.GroupComparator;
import org.apache.hadoop.examples.textpair.KeyComparator;
import org.apache.hadoop.examples.textpair.TextPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Descendant {

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(NaiveDescendant.class);
		String inputPath = args[0];
		String outputPath = args[1];
		String query = args[2].replace(' ', '-');
		int specIteration = Integer.parseInt(args[3]);
		int numReducers = 7;
		if (args.length > 4) {
			numReducers = Integer.parseInt(args[4]);
		}

		long end = 0;
		long start = System.currentTimeMillis();

		/**
		 * Join step
		 */
		JobConf job1 = new JobConf(NaiveDescendant.class);
		job1.setJobName("Descedant Join");
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(JoinMap.class);
		job1.setReducerClass(JoinReduce.class);
		job1.setMapOutputKeyClass(TextPair.class);
		job1.setMapOutputValueClass(TextPair.class);
		job1.setInputFormat(TextInputFormat.class);
		job1.setOutputFormat(TextOutputFormat.class);
		job1.setPartitionerClass(FirstPartitioner.class);
		job1.setOutputKeyComparatorClass(KeyComparator.class);
		job1.setOutputValueGroupingComparator(GroupComparator.class);
		job1.setNumReduceTasks(numReducers);

		/**
		 * Duplicate Removal Step
		 */
		JobConf job2 = new JobConf(NaiveDescendant.class);
		job2.setJobName("Descedant Duplicate Elimination");
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(DuplicateEliminateMap.class);
		job2.setReducerClass(DuplicateEliminateReduce.class);
		job2.setInputFormat(TextInputFormat.class);
		job2.setOutputFormat(TextOutputFormat.class);
		job2.setNumReduceTasks(numReducers);

		/**
		 * set the as-a-whole iterative join job conf
		 */
		conf = new JobConf(Descendant.class);
		conf.setJobName("Iterative Join");
		conf.setNumReduceTasks(numReducers);
		conf.setLoopInputOutput(DescendantLoopInputOutput.class);
		conf.setLoopReduceCacheSwitch(DescendantReduceCacheSwitch.class);
		conf.setLoopReduceCacheFilter(DescendantReduceCacheFilter.class);
		conf.setInputPath(inputPath);
		conf.setOutputPath(outputPath);
		// set up the m-r step pipeline
		conf.setStepConf(0, job1);
		conf.setStepConf(1, job2);
		conf.setIterative(true);
		conf.setNumIterations(specIteration);
		conf.set("descedant.query", query);
		JobClient.runJob(conf);

		/**
		 * aggregate delta relations from each iteration
		 */
		conf = new JobConf(NaiveDescendant.class);
		conf.setJobName("Aggregate Delta Relation");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(AggregateResultMap.class);
		conf.setReducerClass(AggregateResultReduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		int pass = 2 * specIteration;
		for (int i = 1; i < pass; i += 2)
			FileInputFormat.addInputPaths(conf, outputPath + "/i" + i);
		FileOutputFormat
				.setOutputPath(conf, new Path(outputPath + "/i" + pass));
		conf.setNumReduceTasks(numReducers);
		JobClient.runJob(conf);

		end = System.currentTimeMillis();
		System.out.println("running time " + (end - start) / 1000 + "s");
	}

}
