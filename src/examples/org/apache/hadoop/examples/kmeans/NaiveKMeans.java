package org.apache.hadoop.examples.kmeans;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class NaiveKMeans {

	/**
	 * the Mapper class for K-means clustering
	 * 
	 * @author ybu
	 */
	public static class KMeansMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private HashMap<String, double[]> reducerOutput = new HashMap<String, double[]>();

		private double rowBuffer[] = null;

		private Text idBuffer = new Text();

		private Text outputBuffer = new Text();

		public void configure(JobConf conf) {
			int iteration = conf.getCurrentIteration();
			System.out.println("iteration number " + iteration);
			if (iteration <= 0) {
				reducerOutput
						.put(
								"1",
								parseStringToVector("1,4.37432e-10,-0.453421,0.496471,-0.491063,0.385212,-0.145869,0.40665,-0.0101755,1.33315e-05"));
				reducerOutput
						.put(
								"2",
								parseStringToVector("1,4.37432e-10,-0.476908,0.492004,-0.485109,0.477835,-0.370386,0.711489,-0.00969561,1.33315e-05"));
				reducerOutput
						.put(
								"3",
								parseStringToVector("1,4.37432e-10,-0.459093,0.493643,-0.489528,0.4835,-0.289689,0.48659,-0.0100747,1.33315e-05"));

			} else {
				try {
					reducerOutput.clear();
					FileSystem dfs = FileSystem.get(conf);
					System.out.println("mapper current iteration " + iteration);
					String location = conf.getOutputPath() + "/i"
							+ (iteration - 1);
					Path path = new Path(location);
					System.out.println("reducer feedback input path: "
							+ location);

					FileStatus[] files = dfs.listStatus(path);

					for (int i = 0; i < files.length; i++) {
						if (!files[i].isDir()) {
							FSDataInputStream is = dfs.open(files[i].getPath());
							String line = null;
							while ((line = is.readLine()) != null) {
								String fields[] = line.split("\t", 2);
								reducerOutput.put(fields[0],
										parseStringToVector(fields[1]));
								System.out.println(fields[0] + " " + fields[1]);
							}
							is.close();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

				if (reducerOutput.isEmpty())
					System.out.println("init error");
			}
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			if (reducerOutput == null || reducerOutput.isEmpty())
				throw new IOException("reducer output is null");

			String line = value.toString();

			// skip the first line
			if (line.indexOf("::") >= 0)
				return;

			// String dataString = line.substring(pos + 1);
			String dataString = line;
			// System.out.println("mapp string: " + dataString);
			double[] row = parseStringToVector(dataString, rowBuffer);

			Iterator<String> keys = reducerOutput.keySet().iterator();
			double minDistance = Double.MAX_VALUE;
			String minID = "";

			// find the cluster membership for the data vector
			while (keys.hasNext()) {
				String id = keys.next();
				double[] point = reducerOutput.get(id);

				double currentDistance = distance(row, point);
				if (currentDistance < minDistance) {
					minDistance = currentDistance;
					minID = id;
					// minValue = data;
				}
			}

			// System.out.println(dataString);
			// output the vector (value) and cluster membership (key)
			idBuffer.clear();
			idBuffer.append(minID.getBytes(), 0, minID.getBytes().length);

			outputBuffer.clear();
			outputBuffer.append(dataString.getBytes(), 0,
					dataString.getBytes().length);
			output.collect(idBuffer, outputBuffer);
		}

		/**
		 * get the Euclidean distance between two high-dimensional vectors
		 * 
		 * @param d1
		 * @param d2
		 * @return
		 */
		private double distance(double[] d1, double[] d2) {
			double distance = 0;
			int len = d1.length < d2.length ? d1.length : d2.length;

			for (int i = 1; i < len; i++) {
				distance += (d1[i] - d2[i]) * (d1[i] - d2[i]);
			}

			return Math.sqrt(distance);
		}
	}

	/**
	 * parse a string to a high-dimensional double vector
	 * 
	 * @param line
	 * @return
	 */
	private static double[] parseStringToVector(String line, double[] row) {
		try {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			int size = tokenizer.countTokens();

			if (row == null)
				row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}

			return row;
		} catch (Exception e) {
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			int size = tokenizer.countTokens();
			if (row == null)
				row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}

			return row;
		}
	}

	/**
	 * parse a string to a high-dimensional double vector
	 * 
	 * @param line
	 * @return
	 */
	private static double[] parseStringToVector(String line) {
		try {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			int size = tokenizer.countTokens();

			double[] row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}

			return row;
		} catch (Exception e) {
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			int size = tokenizer.countTokens();

			double[] row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}

			return row;
		}
	}

	private static void accumulate(double[] sum, double[] array) {
		sum[0] = 0;
		for (int i = 1; i < sum.length; i++)
			sum[i] += array[i];
	}

	/**
	 * the reducer class for K-means clustering
	 * 
	 * @author ybu
	 * 
	 */
	public static class KMeansReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			double[] sum = null;
			long count = 0;
			if (values.hasNext()) {
				String data = values.next().toString();
				double[] row = parseStringToVector(data);
				sum = new double[row.length];
				accumulate(sum, row);
				count++;
			} else
				return;

			while (values.hasNext()) {
				String data = values.next().toString();
				double[] row = parseStringToVector(data);
				// Ssum = new double[row.length];
				if (row.length < sum.length) {
					System.out.println("less dim: " + data);
					continue;
				}
				accumulate(sum, row);
				count++;
			}

			// generate the new means
			String result = sum[0] + ",";
			for (int i = 1; i <= sum.length - 1; i++) {
				sum[i] = sum[i] / count;
				result += (sum[i] + ",");
			}
			result.trim();

			output.collect(key, new Text(result));
		}
	}

	public static long getInputSize(JobConf conf, Path path) throws Exception {
		FileSystem dfs = FileSystem.get(conf);
		FileStatus[] files = dfs.listStatus(path);

		long size = 0;

		for (int i = 0; i < files.length; i++)
			size += files[i].getLen();
		return size;
	}

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

		int currentIteration = 0;
		long start = System.currentTimeMillis();
		while (currentIteration < specIteration) {
			JobConf conf = new JobConf(KMeans.class);
			conf.setJobName("K-means");

			conf.setMapperClass(KMeansMapper.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			conf.setCombinerClass(KMeansReducer.class);
			conf.setReducerClass(KMeansReducer.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			conf.setNumReduceTasks(numReducers);
			conf.setSpeculativeExecution(false);
			conf.setCurrentIteration(currentIteration);
			conf.setOutputPath(outputPath);
			FileInputFormat.setInputPaths(conf, new Path(inputPath));
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/i"
					+ currentIteration));

			JobClient.runJob(conf);
			currentIteration++;
		}
		long end = System.currentTimeMillis();
		System.out.println("running time " + (end - start) / 1000 + "s");
	}
}