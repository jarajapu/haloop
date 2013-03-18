package org.apache.hadoop.examples.descendant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.examples.textpair.FirstPartitioner;
import org.apache.hadoop.examples.textpair.GroupComparator;
import org.apache.hadoop.examples.textpair.KeyComparator;
import org.apache.hadoop.examples.textpair.TextPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class NaiveDescendant {

	/**
	 * tag for right table tuple
	 */
	private static String tag0 = "0";

	/**
	 * tag for left table tuple
	 */
	private static String tag1 = "1";

	private static boolean stringStartWith(String s1, String s2) {
		return s1.startsWith(s2);
	}

	public static class JoinMap extends MapReduceBase implements
			Mapper<LongWritable, Text, TextPair, TextPair> {

		/**
		 * current iteration
		 */
		private int currentIteration = 0;

		/**
		 * query string
		 */
		private String query = null;

		/**
		 * the token list
		 */
		List<String> tokenList = new ArrayList<String>();

		/**
		 * map output key
		 */
		TextPair outputKey = new TextPair(new Text(), new Text());

		/**
		 * map output value
		 */
		TextPair outputValue = new TextPair(new Text(), new Text());

		public void configure(JobConf job) {
			currentIteration = job.getCurrentIteration();
			query = job.get("descedant.query");
		}

		public JoinMap() {
			super();
		}

		public void map(LongWritable key, Text value,
				OutputCollector<TextPair, TextPair> output, Reporter reporter)
				throws IOException {
			tokenList.clear();

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens())
				tokenList.add(tokenizer.nextToken().trim());

			String leftStr = tokenList.get(0);
			String rightStr = tokenList.get(1);

			int lineSize = tokenList.size();

			if (currentIteration == 0) {
				if (stringStartWith(leftStr, query)) {
					outputKey.setFirstText(rightStr);
					outputKey.setSecondText(tag0);
					outputValue.setFirstText(leftStr);
					outputValue.setSecondText(tag0);
					output.collect(outputKey, outputValue);
				}
				outputKey.setFirstText(leftStr);
				outputKey.setSecondText(tag1);
				outputValue.setFirstText(rightStr);
				outputValue.setSecondText(tag1);
				output.collect(outputKey, outputValue);
			} else {
				if (lineSize == 3) {
					outputKey.setFirstText(rightStr);
					outputKey.setSecondText(tag0);
					outputValue.setFirstText(leftStr);
					outputValue.setSecondText(tag0);
					output.collect(outputKey, outputValue);
				}
				if (lineSize == 2) {
					outputKey.setFirstText(leftStr);
					outputKey.setSecondText(tag1);
					outputValue.setFirstText(rightStr);
					outputValue.setSecondText(tag1);
					output.collect(outputKey, outputValue);
				}
			}
		}
	}

	public static class JoinReduce extends MapReduceBase implements
			Reducer<TextPair, TextPair, Text, Text> {
		// left table in join
		private List<String> cachedLeft = new ArrayList<String>();

		private Text tagZeroText = new Text(tag0);

		private Text tagOneText = new Text(tag1);

		private Text outputKey = new Text();

		/**
		 * join the two lists
		 * 
		 * @param outer
		 * @param inner
		 * @param output
		 * @throws IOException
		 */
		private void join(List<String> outer, Text inner,
				OutputCollector<Text, Text> output) throws IOException {
			// System.out.println("in join");
			for (String outerStr : outer) {
				outputKey.set(outerStr.getBytes());
				if (!outputKey.equals(inner))
					output.collect(outputKey, inner);
			}
		}

		public void reduce(TextPair key, Iterator<TextPair> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// clear cache
			cachedLeft.clear();
			int rightCount = 0;

			while (values.hasNext()) {
				TextPair value = values.next();
				if (value.getSecond().equals(tagZeroText)) {
					System.out.println(value.getFirst());
					// put the left into cache
					cachedLeft.add(value.getFirst().toString());
				} else if (value.getSecond().equals(tagOneText)) {
					join(cachedLeft, value.getFirst(), output);
					rightCount++;
				} else {
					throw new IllegalStateException("error tag "
							+ value.getSecond());
				}
			}
		}
	}

	public static class DuplicateEliminateMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private List<String> tokenList = new ArrayList<String>();

		private Text outputKey = new Text();

		private Text outputValue = new Text();

		private int currentIteration;

		@Override
		public void configure(JobConf job) {
			currentIteration = job.getCurrentIteration();
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			tokenList.clear();

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens())
				tokenList.add(tokenizer.nextToken());

			String valueGeneratedIteration = Integer.toString(currentIteration);

			if (tokenList.size() >= 3)
				valueGeneratedIteration = tokenList.get(2);

			String keyStr = tokenList.get(0) + " " + tokenList.get(1);

			outputKey.set(keyStr.getBytes());
			outputValue.set(valueGeneratedIteration.getBytes());

			/**
			 * token 0, key token 1, value token 2, iteration (if iteration > 0)
			 */
			output.collect(outputKey, outputValue);
		}
	}

	public static class DuplicateEliminateReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		private int currentIteration = 0;

		private Text outputKey = new Text();

		private Text outputValue = new Text();

		@Override
		public void configure(JobConf job) {
			currentIteration = job.getCurrentIteration();
		}

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] valueString = key.toString().split(" ");
			boolean newAppear = false;
			boolean oldAppear = false;

			while (values.hasNext()) {
				int dataIteration = Integer.parseInt(values.next().toString());
				if (dataIteration == currentIteration)
					newAppear = true;
				else
					oldAppear = true;
				if (newAppear && oldAppear)
					break;
			}

			/**
			 * only output unique values in latest generation
			 */
			if (newAppear && !oldAppear) {
				outputKey.set(valueString[0].getBytes());
				String valueStr = valueString[1] + "\t" + currentIteration;
				outputValue.set(valueStr.getBytes());
				output.collect(outputKey, outputValue);
			}
		}
	}

	public static class AggregateResultMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private List<String> tokenList = new ArrayList<String>();

		private Text outputKey = new Text();

		private Text outputValue = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			tokenList.clear();

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens())
				tokenList.add(tokenizer.nextToken());

			String keyStr = tokenList.get(0);
			String valueStr = tokenList.get(1);

			outputKey.set(keyStr.getBytes());
			outputValue.set(valueStr.getBytes());

			/**
			 * key token 1, value token 2,
			 */
			output.collect(outputKey, outputValue);
		}
	}

	public static class AggregateResultReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				Text value = values.next();
				output.collect(key, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		int specIteration = Integer.MAX_VALUE;
		JobConf conf = new JobConf(NaiveDescendant.class);
		String inputPath = args[0];
		String outputPath = args[1];
		String query = args[2].replace(' ', '-');
		if (args.length > 3) {
			specIteration = Integer.parseInt(args[3]);
		}

		int numReducers = 5;

		if (args.length > 4) {
			numReducers = Integer.parseInt(args[4]);
		}

		long start = 0;
		long end = 0;
		int pass = 0;
		int iteration = 0;

		start = System.currentTimeMillis();
		do {
			/**************** Join Job **********************/
			conf = new JobConf(NaiveDescendant.class);
			conf.setCurrentIteration(iteration);
			conf.setJobName("Descedant Join");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(JoinMap.class);
			conf.setReducerClass(JoinReduce.class);
			conf.setMapOutputKeyClass(TextPair.class);
			conf.setMapOutputValueClass(TextPair.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			conf.setPartitionerClass(FirstPartitioner.class);
			conf.setOutputKeyComparatorClass(KeyComparator.class);
			conf.setOutputValueGroupingComparator(GroupComparator.class);
			conf.set("descedant.query", query);
			FileInputFormat.setInputPaths(conf, new Path(inputPath));

			if (pass > 0)
				FileInputFormat.addInputPath(conf, new Path(outputPath + "/i"
						+ (pass - 1)));
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/i"
					+ pass));
			conf.setNumReduceTasks(numReducers);
			JobClient.runJob(conf);
			pass++;

			/************** Duplicate Removal Job **************/
			conf = new JobConf(NaiveDescendant.class);
			conf.setCurrentIteration(iteration);
			conf.setJobName("Descedant Duplicate Elimination");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(DuplicateEliminateMap.class);
			conf.setReducerClass(DuplicateEliminateReduce.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(conf, outputPath + "/i" + (pass - 1));
			for (int i = 1; i < pass; i += 2)
				FileInputFormat.addInputPaths(conf, outputPath + "/i" + i);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/i"
					+ pass));
			conf.setNumReduceTasks(numReducers);
			JobClient.runJob(conf);
			pass++;
			iteration = pass / 2;
		} while (iteration < specIteration);

		conf = new JobConf(NaiveDescendant.class);
		conf.setJobName("Aggregate Result Elimination");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(AggregateResultMap.class);
		conf.setReducerClass(AggregateResultReduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
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
