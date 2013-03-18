/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.iterative.DefaultDistanceMeasure;
import org.apache.hadoop.mapred.iterative.DefaultLoopInputOutput;
import org.apache.hadoop.mapred.iterative.DefaultLoopMapCacheFilter;
import org.apache.hadoop.mapred.iterative.DefaultLoopMapCacheSwitch;
import org.apache.hadoop.mapred.iterative.DefaultLoopReduceCacheFilter;
import org.apache.hadoop.mapred.iterative.DefaultLoopReduceCacheSwitch;
import org.apache.hadoop.mapred.iterative.DefaultLoopReduceOutputCacheFilter;
import org.apache.hadoop.mapred.iterative.DefaultLoopReduceOutputCacheSwitch;
import org.apache.hadoop.mapred.iterative.DefaultLoopStepHook;
import org.apache.hadoop.mapred.iterative.IDistanceMeasure;
import org.apache.hadoop.mapred.iterative.IFixPointChecker;
import org.apache.hadoop.mapred.iterative.LoopInputOutput;
import org.apache.hadoop.mapred.iterative.LoopMapCacheFilter;
import org.apache.hadoop.mapred.iterative.LoopMapCacheSwitch;
import org.apache.hadoop.mapred.iterative.LoopReduceCacheFilter;
import org.apache.hadoop.mapred.iterative.LoopReduceCacheSwitch;
import org.apache.hadoop.mapred.iterative.LoopReduceOutputCacheFilter;
import org.apache.hadoop.mapred.iterative.LoopReduceOutputCacheSwitch;
import org.apache.hadoop.mapred.iterative.LoopStepHook;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.KeyFieldBasedComparator;
import org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;

/**
 * A map/reduce job configuration.
 * 
 * <p>
 * <code>JobConf</code> is the primary interface for a user to describe a
 * map-reduce job to the Hadoop framework for execution. The framework tries to
 * faithfully execute the job as-is described by <code>JobConf</code>, however:
 * <ol>
 * <li>
 * Some configuration parameters might have been marked as <a href="{@docRoot}
 * /org/apache/hadoop/conf/Configuration.html#FinalParams"> final</a> by
 * administrators and hence cannot be altered.</li>
 * <li>
 * While some job parameters are straight-forward to set (e.g.
 * {@link #setNumReduceTasks(int)}), some parameters interact subtly rest of the
 * framework and/or job-configuration and is relatively more complex for the
 * user to control finely (e.g. {@link #setNumMapTasks(int)}).</li>
 * </ol>
 * </p>
 * 
 * <p>
 * <code>JobConf</code> typically specifies the {@link Mapper}, combiner (if
 * any), {@link Partitioner}, {@link Reducer}, {@link InputFormat} and
 * {@link OutputFormat} implementations to be used etc.
 * 
 * <p>
 * Optionally <code>JobConf</code> is used to specify other advanced facets of
 * the job such as <code>Comparator</code>s to be used, files to be put in the
 * {@link DistributedCache}, whether or not intermediate and/or job outputs are
 * to be compressed (and how), debugability via user-provided scripts (
 * {@link #setMapDebugScript(String)}/{@link #setReduceDebugScript(String)}),
 * for doing post-processing on task logs, task's stdout, stderr, syslog. and
 * etc.
 * </p>
 * 
 * <p>
 * Here is an example on how to configure a job via <code>JobConf</code>:
 * </p>
 * <p>
 * <blockquote>
 * 
 * <pre>
 * // Create a new JobConf
 * JobConf job = new JobConf(new Configuration(), MyJob.class);
 * 
 * // Specify various job-specific parameters
 * job.setJobName(&quot;myjob&quot;);
 * 
 * FileInputFormat.setInputPaths(job, new Path(&quot;in&quot;));
 * FileOutputFormat.setOutputPath(job, new Path(&quot;out&quot;));
 * 
 * job.setMapperClass(MyJob.MyMapper.class);
 * job.setCombinerClass(MyJob.MyReducer.class);
 * job.setReducerClass(MyJob.MyReducer.class);
 * 
 * job.setInputFormat(SequenceFileInputFormat.class);
 * job.setOutputFormat(SequenceFileOutputFormat.class);
 * </pre>
 * 
 * </blockquote>
 * </p>
 * 
 * @see JobClient
 * @see ClusterStatus
 * @see Tool
 * @see DistributedCache
 */
public class JobConf extends Configuration {
	// HaLoop: iterative job configurations
	/**
	 * the step of current running map/reduce job
	 */
	private int currentStep = 0;

	/**
	 * the iteration number of current running loop body
	 */
	private int currentIteration = 0;

	/**
	 * map the step number to mapper output key class
	 */
	private HashMap<Integer, JobConf> stepToConf = new HashMap<Integer, JobConf>();

	JobConf getStepConf(int step) {
		return stepToConf.get(step);
	}

	/**
	 * set class loader
	 */
	@Override
	public void setClassLoader(ClassLoader classLoader) {
		super.setClassLoader(classLoader);
		Iterator<JobConf> iterator = stepToConf.values().iterator();

		while (iterator.hasNext()) {
			JobConf conf = iterator.next();
			conf.setClassLoader(classLoader);
		}
	}

	/**
	 * assoicate a step with JobConf
	 */
	public void setStepConf(int pos, JobConf conf) {
		stepToConf.put(pos, conf);
		setInt("haloop.num.step", stepToConf.size());
	}

	/**
	 * write the JobConf instance to output
	 */
	public void write(DataOutput out) throws IOException {
		super.write(out);
		Iterator<JobConf> confs = stepToConf.values().iterator();
		int i = 0;
		int size = stepToConf.values().size();
		System.out.println("write conf size " + size);
		out.writeInt(size);
		while (confs.hasNext()) {
			JobConf conf = confs.next();
			conf.write(out);
			i++;
		}
	}

	/**
	 * read the JobConf instance fields from input
	 */
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		int size = in.readInt();
		System.out.println("read conf size " + size);

		for (int i = 0; i < size; i++) {
			JobConf conf = new JobConf();
			conf.readFields(in);
			stepToConf.put(i, conf);
		}
	}

	/**
	 * set the the Loop input output
	 * 
	 * @param input
	 */
	public void setLoopInputOutput(Class<? extends LoopInputOutput> inputOutput) {
		setClass("haloop.loopinputoutput", inputOutput, LoopInputOutput.class);
	}

	/**
	 * set the the LoopInput
	 * 
	 * @param input
	 */
	public Class<? extends LoopInputOutput> getLoopInputOutput() {
		return getClass("haloop.loopinputoutput", DefaultLoopInputOutput.class,
				LoopInputOutput.class);
	}

	/**
	 * set the reduce input cache switch
	 * 
	 * @param input
	 */
	public void setLoopReduceCacheSwitch(
			Class<? extends LoopReduceCacheSwitch> cacheControl) {
		setClass("haloop.loopcachecontrol", cacheControl,
				LoopReduceCacheSwitch.class);
	}

	/**
	 * set the the reduce input cache switch
	 * 
	 * @param input
	 */
	public Class<? extends LoopReduceCacheSwitch> getLoopReduceCacheSwitch() {
		return getClass("haloop.loopcachecontrol",
				DefaultLoopReduceCacheSwitch.class, LoopReduceCacheSwitch.class);
	}

	/**
	 * set the loop output cache switch
	 * 
	 * @param input
	 */
	public void setLoopReduceOutputCacheSwitch(
			Class<? extends LoopReduceOutputCacheSwitch> cacheControl) {
		setClass("haloop.loopcachecontrol.output", cacheControl,
				LoopReduceOutputCacheSwitch.class);
	}

	/**
	 * set the the loop output cache switch
	 * 
	 * @param input
	 */
	public Class<? extends LoopReduceOutputCacheSwitch> getLoopReduceOutputCacheSwitch() {
		return getClass("haloop.loopcachecontrol.output",
				DefaultLoopReduceOutputCacheSwitch.class,
				LoopReduceOutputCacheSwitch.class);
	}

	/**
	 * set the loop step hook
	 * 
	 * @param input
	 */
	public void setLoopStepHook(Class<? extends LoopStepHook> hook) {
		setClass("haloop.step.hook", hook, LoopStepHook.class);
	}

	/**
	 * set the loop step hook
	 * 
	 * @param input
	 */
	public Class<? extends LoopStepHook> getLoopStepHook() {
		return getClass("haloop.step.hook", DefaultLoopStepHook.class,
				LoopStepHook.class);
	}

	/**
	 * set the distance measure
	 * 
	 * @param dm
	 */
	public void setDistanceMeasure(Class<? extends IDistanceMeasure> dm) {
		setClass("haloop.fixpoint.distance", dm, IDistanceMeasure.class);
	}

	/**
	 * get the distance measure
	 * 
	 * @param dm
	 */
	public Class<? extends IDistanceMeasure> getDistanceMeasure() {
		return getClass("haloop.fixpoint.distance",
				DefaultDistanceMeasure.class, IDistanceMeasure.class);
	}

	/**
	 * set the distance measure
	 * 
	 * @param dm
	 */
	public void setFixPointChecker(Class<? extends IFixPointChecker> fc) {
		setClass("haloop.fixpoint.checker", fc, IFixPointChecker.class);
	}

	/**
	 * get the distance measure
	 * 
	 * @param dm
	 */
	public Class<? extends IFixPointChecker> getFixpointChecker() {
		return getClass("haloop.fixpoint.checker", FixedPointChecker.class,
				IFixPointChecker.class);
	}

	/**
	 * set fix point check bit
	 * 
	 * @param b
	 */
	public void setFixpointCheck(boolean b) {
		this.setBoolean("haloop.fixpoint.checkbit", b);
	}

	/**
	 * get if fix point should be checked
	 * @return
	 */
	public boolean getFixpointCheck() {
		return this.getBoolean("haloop.fixpoint.checkbit", false);
	}

	/**
	 * set fix point distance
	 */
	public void setFixpointThreshold(float t) {
		this.setFloat("haloop.fixpoint.threshold", t);
	}

	/**
	 * get fix point distance
	 */
	public float getFixpointThreshold() {
		return getFloat("haloop.fixpoint.threshold", Float.MAX_VALUE);
	}

	/**
	 * set the the LoopReduceCacheFilter
	 * 
	 * @param input
	 */
	public void setLoopReduceCacheFilter(
			Class<? extends LoopReduceCacheFilter> cacheFilter) {
		setClass("haloop.loopcachefilter", cacheFilter,
				LoopReduceCacheFilter.class);
	}

	/**
	 * get the the LooReduceCacheFilter class
	 * 
	 * @param input
	 */
	public Class<? extends LoopReduceCacheFilter> getLoopReduceCacheFilter() {
		return getClass("haloop.loopcachefilter",
				DefaultLoopReduceCacheFilter.class, LoopReduceCacheFilter.class);
	}

	/**
	 * set the the LoopReduceCacheFilter
	 * 
	 * @param input
	 */
	public void setLoopReduceOutputCacheFilter(
			Class<? extends LoopReduceOutputCacheFilter> cacheFilter) {
		setClass("haloop.loopcachefilter.output", cacheFilter,
				LoopReduceOutputCacheFilter.class);
	}

	/**
	 * get the the LooReduceCacheFilter class
	 * 
	 * @param input
	 */
	public Class<? extends LoopReduceOutputCacheFilter> getLoopReduceOutputCacheFilter() {
		return getClass("haloop.loopcachefilter.output",
				DefaultLoopReduceOutputCacheFilter.class,
				LoopReduceOutputCacheFilter.class);
	}

	/**
	 * set the the LoopMapCacheSwitch
	 * 
	 * @param input
	 */
	public void setLoopMapCacheSwitch(
			Class<? extends LoopMapCacheSwitch> cacheControl) {
		setClass("haloop.map.loopcachecontrol", cacheControl,
				LoopMapCacheSwitch.class);
	}

	/**
	 * get the the LoopMapCacheSwitch
	 * 
	 * @param input
	 */
	public Class<? extends LoopMapCacheSwitch> getLoopMapCacheSwitch() {
		return getClass("haloop.map.loopcachecontrol",
				DefaultLoopMapCacheSwitch.class, LoopMapCacheSwitch.class);
	}

	/**
	 * set the the LoopMapCacheFilter
	 * 
	 * @param input
	 */
	public void setLoopMapCacheFilter(
			Class<? extends LoopMapCacheFilter> cacheFilter) {
		setClass("haloop.map.loopcachefilter", cacheFilter,
				LoopMapCacheFilter.class);
	}

	/**
	 * set the the LoopInput
	 * 
	 * @param input
	 */
	public Class<? extends LoopMapCacheFilter> getLoopMapCacheFilter() {
		return getClass("haloop.map.loopcachefilter",
				DefaultLoopMapCacheFilter.class, LoopMapCacheFilter.class);
	}

	/**
	 * get the number of steps in loop body
	 * 
	 * @return the number of steps in loop body
	 */
	public int getNumberOfLoopBodySteps() {
		return getInt("haloop.num.step", 0);
	}

	/**
	 * set the mapper cache option, by default, it is disabled
	 * 
	 * @param enable
	 *            enable or not
	 */
	public void setMapperInputCacheOption(boolean enable) {
		this.setBoolean("mappercache", enable);
	}

	/**
	 * get the mapper cache option, by default, it is disabled
	 * 
	 * @return mapper input option
	 */
	public boolean getMapperInputCacheOption() {
		return this.getBoolean("mappercache", false);
	}

	/**
	 * set the mapper cache option, by default, it is disabled
	 * 
	 * @param enable
	 *            enable or not
	 */
	public void setReducerOutputCacheOption(boolean enable) {
		this.setBoolean("reducer.output.cache", enable);
	}

	/**
	 * get the mapper cache option, by default, it is disabled
	 * 
	 * @return mapper input option
	 */
	public boolean getReducerOutputCacheOption() {
		return this.getBoolean("reducer.output.cache", false);
	}

	/**
	 * get current step number
	 * 
	 * @return
	 */
	public int getCurrentStep() {
		return this.getInt("haloop.currentStep", 0);
	}

	/**
	 * set current step
	 * 
	 * @param step
	 */
	public void setCurrentStep(int step) {
		currentStep = step;
		this.setInt("haloop.currentStep", step);
	}

	/**
	 * get current iteration number
	 * 
	 * @return
	 */
	public int getCurrentIteration() {
		return this.getInt("haloop.currentiteration", 0);
	}

	/**
	 * set current iteration
	 * 
	 * @param i
	 */
	public void setCurrentIteration(int i) {
		currentIteration = i;
		this.setInt("haloop.currentiteration", i);
	}

	public void setOutputPath(String path) {
		set("haloop.output.path", path);
	}

	public String getOutputPath() {
		return get("haloop.output.path", "");
	}

	public void setInputPath(String path) {
		set("haloop.input.path", path);
	}

	public String getInputPath() {
		return get("haloop.input.path", "");
	}

	public JobConf duplicate() throws IOException {
		JobConf job = new JobConf();

		DataOutputBuffer outputBuffer = new DataOutputBuffer();
		write(outputBuffer);

		DataInputBuffer inputBuffer = new DataInputBuffer();
		inputBuffer.reset(outputBuffer.getData(), 0, outputBuffer.getLength());
		job.readFields(inputBuffer);

		return job;
	}

	/**
	 * only used in recovery task
	 * 
	 * @param iteration
	 * @param step
	 */
	public void setCurrentIterationAndStep(int iteration, int step) {
		/**
		 * move to the next step/iteration
		 * 
		 * @return true if not end; false if end
		 */
		currentIteration = iteration;
		currentStep = step;
		setCurrentIteration(currentIteration);
		setCurrentStep(currentStep);

		JobConf currentConf = stepToConf.get(currentStep);
		setMapperClass(currentConf.getMapperClass());
		setReducerClass(currentConf.getReducerClass());

		Class combiner = currentConf.getCombinerClass();
		if (combiner != null)
			setCombinerClass(combiner);

		setOutputKeyClass(currentConf.getOutputKeyClass());
		setOutputValueClass(currentConf.getOutputValueClass());
		setInputFormat(currentConf.getInputFormat().getClass());
		setOutputFormat(currentConf.getOutputFormat().getClass());
		setMapOutputKeyClass(currentConf.getMapOutputKeyClass());
		setMapOutputValueClass(currentConf.getMapOutputValueClass());

		setPartitionerClass(currentConf.getPartitionerClass());
		setOutputKeyComparatorClass(currentConf.getClass(
				"mapred.output.key.comparator.class", Text.Comparator.class,
				RawComparator.class));
		setOutputValueGroupingComparator(currentConf.getClass(
				"mapred.output.value.groupfn.class", Text.Comparator.class,
				RawComparator.class));
	}

	/**
	 * move to the next step/iteration
	 * 
	 * @return true if not end; false if end
	 */
	public void next() {

		int numberOfSteps = stepToConf.values().size();
		if (currentStep + 1 >= numberOfSteps) {
			currentIteration++;
			currentStep = 0;
		} else
			currentStep++;

		// set current iteration
		setCurrentIteration(currentIteration);
		setCurrentStep(currentStep);

		JobConf currentConf = stepToConf.get(currentStep);
		setMapperClass(currentConf.getMapperClass());
		setReducerClass(currentConf.getReducerClass());
		setCombinerClass(currentConf.getCombinerClass());

		setOutputKeyClass(currentConf.getOutputKeyClass());
		setOutputValueClass(currentConf.getOutputValueClass());
		setInputFormat(currentConf.getInputFormat().getClass());
		setOutputFormat(currentConf.getOutputFormat().getClass());
		setMapOutputKeyClass(currentConf.getMapOutputKeyClass());
		setMapOutputValueClass(currentConf.getMapOutputValueClass());

		setPartitionerClass(currentConf.getPartitionerClass());
		setOutputKeyComparatorClass(currentConf.getClass(
				"mapred.output.key.comparator.class", Text.Comparator.class,
				RawComparator.class));
		setOutputValueGroupingComparator(currentConf.getClass(
				"mapred.output.value.groupfn.class", Text.Comparator.class,
				RawComparator.class));
	}

	/**
	 * move to the next step/iteration
	 * 
	 * @return true if not end; false if end
	 */
	public void init() {
		// set current iteration
		setCurrentIteration(currentIteration);
		setCurrentStep(currentStep);

		JobConf currentConf = stepToConf.get(currentStep);
		setMapperClass(currentConf.getMapperClass());
		setReducerClass(currentConf.getReducerClass());

		Class combiner = currentConf.getCombinerClass();
		if (combiner != null)
			setCombinerClass(combiner);
		setOutputKeyClass(currentConf.getOutputKeyClass());
		setOutputValueClass(currentConf.getOutputValueClass());
		setInputFormat(currentConf.getInputFormat().getClass());
		setOutputFormat(currentConf.getOutputFormat().getClass());
		setMapOutputKeyClass(currentConf.getMapOutputKeyClass());
		setMapOutputValueClass(currentConf.getMapOutputValueClass());

		setMapOutputKeyClass(currentConf.getMapOutputKeyClass());
		setPartitionerClass(currentConf.getPartitionerClass());
		setOutputKeyComparatorClass(currentConf.getClass(
				"mapred.output.key.comparator.class", Text.Comparator.class,
				RawComparator.class));
		setOutputValueGroupingComparator(currentConf.getClass(
				"mapred.output.value.groupfn.class", Text.Comparator.class,
				RawComparator.class));
	}

	/**
	 * whether there is next iteration or not
	 */
	public boolean hasNext() {
		int numberOfSteps = stepToConf.values().size();
		if (currentStep + 1 < numberOfSteps)
			return true;
		else if (currentIteration + 1 < getNumIterations())
			return true;
		else
			return false;
	}

	private static final Log LOG = LogFactory.getLog(JobConf.class);

	static {
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public static final String MAPRED_TASK_MAXVMEM_PROPERTY = "mapred.task.maxvmem";

	/**
	 * @deprecated
	 */
	@Deprecated
	public static final String UPPER_LIMIT_ON_TASK_VMEM_PROPERTY = "mapred.task.limit.maxvmem";

	/**
	 * @deprecated
	 */
	@Deprecated
	public static final String MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY = "mapred.task.default.maxvmem";

	/**
	 * @deprecated
	 */
	@Deprecated
	public static final String MAPRED_TASK_MAXPMEM_PROPERTY = "mapred.task.maxpmem";

	/**
	 * A value which if set for memory related configuration options, indicates
	 * that the options are turned off.
	 */
	public static final long DISABLED_MEMORY_LIMIT = -1L;

	/**
	 * Name of the queue to which jobs will be submitted, if no queue name is
	 * mentioned.
	 */
	public static final String DEFAULT_QUEUE_NAME = "default";

	static final String MAPRED_JOB_MAP_MEMORY_MB_PROPERTY = "mapred.job.map.memory.mb";

	static final String MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY = "mapred.job.reduce.memory.mb";

	// Yingyi: set the distance threshold
	public void setDistanceThreshold(float threshold) {
		set("mapred.fixedpoint.threshold", Float.toString(threshold));
	}

	public float getDistanceThreshold() {
		return getFloat("mapred.fixedpoint.threshold", -1);
	}

	// Yingyi: set the job is iterative or not
	public void setIterative(boolean iterative) {
		set("mapred.fixedpoint.iterative", Boolean.toString(iterative));
	}

	public boolean isIterative() {
		return getBoolean("mapred.fixedpoint.iterative", false);
	}

	// Yingyi: set the number of iteration rounds
	public void setNumIterations(int num) {
		set("mapred.fixedpoint.iteartions", Integer.toString(num));
	}

	public int getNumIterations() {
		return getInt("mapred.fixedpoint.iteartions", 0);
	}

	/**
	 * Construct a map/reduce job configuration.
	 */
	public JobConf() {
		checkAndWarnDeprecation();
	}

	/**
	 * Construct a map/reduce job configuration.
	 * 
	 * @param exampleClass
	 *            a class whose containing jar is used as the job's jar.
	 */
	public JobConf(Class exampleClass) {
		setJarByClass(exampleClass);
		checkAndWarnDeprecation();
	}

	/**
	 * Construct a map/reduce job configuration.
	 * 
	 * @param conf
	 *            a Configuration whose settings will be inherited.
	 */
	public JobConf(Configuration conf) {
		super(conf);
		checkAndWarnDeprecation();
	}

	/**
	 * Construct a map/reduce job configuration.
	 * 
	 * @param conf
	 *            a Configuration whose settings will be inherited.
	 * @param exampleClass
	 *            a class whose containing jar is used as the job's jar.
	 */
	public JobConf(Configuration conf, Class exampleClass) {
		this(conf);
		setJarByClass(exampleClass);
	}

	/**
	 * Construct a map/reduce configuration.
	 * 
	 * @param config
	 *            a Configuration-format XML job description file.
	 */
	public JobConf(String config) {
		this(new Path(config));
	}

	/**
	 * Construct a map/reduce configuration.
	 * 
	 * @param config
	 *            a Configuration-format XML job description file.
	 */
	public JobConf(Path config) {
		super();
		addResource(config);
		checkAndWarnDeprecation();
	}

	/**
	 * A new map/reduce configuration where the behavior of reading from the
	 * default resources can be turned off.
	 * <p/>
	 * If the parameter {@code loadDefaults} is false, the new instance will not
	 * load resources from the default files.
	 * 
	 * @param loadDefaults
	 *            specifies whether to load from the default files
	 */
	public JobConf(boolean loadDefaults) {
		super(loadDefaults);
		checkAndWarnDeprecation();
	}

	/**
	 * Get the user jar for the map-reduce job.
	 * 
	 * @return the user jar for the map-reduce job.
	 */
	public String getJar() {
		return get("mapred.jar");
	}

	/**
	 * Set the user jar for the map-reduce job.
	 * 
	 * @param jar
	 *            the user jar for the map-reduce job.
	 */
	public void setJar(String jar) {
		set("mapred.jar", jar);
	}

	public void setQuery(String query) {
		set("mapred.query", query);
	}

	public String getQuery() {
		return get("mapred.query", "");
	}

	/**
	 * Set the job's jar file by finding an example class location.
	 * 
	 * @param cls
	 *            the example class.
	 */
	public void setJarByClass(Class cls) {
		String jar = findContainingJar(cls);
		if (jar != null) {
			setJar(jar);
		}
	}

	public String[] getLocalDirs() throws IOException {
		return getStrings("mapred.local.dir");
	}

	public void deleteLocalFiles() throws IOException {
		String[] localDirs = getLocalDirs();
		for (int i = 0; i < localDirs.length; i++) {
			FileSystem.getLocal(this).delete(new Path(localDirs[i]));
		}
	}

	public void deleteLocalFiles(String subdir) throws IOException {
		String[] localDirs = getLocalDirs();
		for (int i = 0; i < localDirs.length; i++) {
			FileSystem.getLocal(this).delete(new Path(localDirs[i], subdir));
		}
	}

	/**
	 * Constructs a local file name. Files are distributed among configured
	 * local directories.
	 */
	public Path getLocalPath(String pathString) throws IOException {
		return getLocalPath("mapred.local.dir", pathString);
	}

	/**
	 * Get the reported username for this job.
	 * 
	 * @return the username
	 */
	public String getUser() {
		return get("user.name");
	}

	/**
	 * Set the reported username for this job.
	 * 
	 * @param user
	 *            the username for this job.
	 */
	public void setUser(String user) {
		set("user.name", user);
	}

	/**
	 * Set whether the framework should keep the intermediate files for failed
	 * tasks.
	 * 
	 * @param keep
	 *            <code>true</code> if framework should keep the intermediate
	 *            files for failed tasks, <code>false</code> otherwise.
	 * 
	 */
	public void setKeepFailedTaskFiles(boolean keep) {
		setBoolean("keep.failed.task.files", keep);
	}

	/**
	 * Should the temporary files for failed tasks be kept?
	 * 
	 * @return should the files be kept?
	 */
	public boolean getKeepFailedTaskFiles() {
		return getBoolean("keep.failed.task.files", false);
	}

	/**
	 * Set a regular expression for task names that should be kept. The regular
	 * expression ".*_m_000123_0" would keep the files for the first instance of
	 * map 123 that ran.
	 * 
	 * @param pattern
	 *            the java.util.regex.Pattern to match against the task names.
	 */
	public void setKeepTaskFilesPattern(String pattern) {
		set("keep.task.files.pattern", pattern);
	}

	/**
	 * Get the regular expression that is matched against the task names to see
	 * if we need to keep the files.
	 * 
	 * @return the pattern as a string, if it was set, othewise null.
	 */
	public String getKeepTaskFilesPattern() {
		return get("keep.task.files.pattern");
	}

	/**
	 * Set the current working directory for the default file system.
	 * 
	 * @param dir
	 *            the new current working directory.
	 */
	public void setWorkingDirectory(Path dir) {
		dir = new Path(getWorkingDirectory(), dir);
		set("mapred.working.dir", dir.toString());
	}

	/**
	 * Get the current working directory for the default file system.
	 * 
	 * @return the directory name.
	 */
	public Path getWorkingDirectory() {
		String name = get("mapred.working.dir");
		if (name != null) {
			return new Path(name);
		} else {
			try {
				Path dir = FileSystem.get(this).getWorkingDirectory();
				set("mapred.working.dir", dir.toString());
				return dir;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Sets the number of tasks that a spawned task JVM should run before it
	 * exits
	 * 
	 * @param numTasks
	 *            the number of tasks to execute; defaults to 1; -1 signifies no
	 *            limit
	 */
	public void setNumTasksToExecutePerJvm(int numTasks) {
		setInt("mapred.job.reuse.jvm.num.tasks", numTasks);
	}

	/**
	 * Get the number of tasks that a spawned JVM should execute
	 */
	public int getNumTasksToExecutePerJvm() {
		return getInt("mapred.job.reuse.jvm.num.tasks", 1);
	}

	/**
	 * Get the {@link InputFormat} implementation for the map-reduce job,
	 * defaults to {@link TextInputFormat} if not specified explicity.
	 * 
	 * @return the {@link InputFormat} implementation for the map-reduce job.
	 */
	public InputFormat getInputFormat() {
		return ReflectionUtils.newInstance(
				getClass("mapred.input.format.class", TextInputFormat.class,
						InputFormat.class), this);
	}

	/**
	 * Set the {@link InputFormat} implementation for the map-reduce job.
	 * 
	 * @param theClass
	 *            the {@link InputFormat} implementation for the map-reduce job.
	 */
	public void setInputFormat(Class<? extends InputFormat> theClass) {
		setClass("mapred.input.format.class", theClass, InputFormat.class);
	}

	/**
	 * Get the {@link OutputFormat} implementation for the map-reduce job,
	 * defaults to {@link TextOutputFormat} if not specified explicity.
	 * 
	 * @return the {@link OutputFormat} implementation for the map-reduce job.
	 */
	public OutputFormat getOutputFormat() {
		return ReflectionUtils.newInstance(
				getClass("mapred.output.format.class", TextOutputFormat.class,
						OutputFormat.class), this);
	}

	/**
	 * Get the {@link OutputCommitter} implementation for the map-reduce job,
	 * defaults to {@link FileOutputCommitter} if not specified explicitly.
	 * 
	 * @return the {@link OutputCommitter} implementation for the map-reduce
	 *         job.
	 */
	public OutputCommitter getOutputCommitter() {
		return (OutputCommitter) ReflectionUtils
				.newInstance(
						getClass("mapred.output.committer.class",
								FileOutputCommitter.class,
								OutputCommitter.class), this);
	}

	/**
	 * Set the {@link OutputCommitter} implementation for the map-reduce job.
	 * 
	 * @param theClass
	 *            the {@link OutputCommitter} implementation for the map-reduce
	 *            job.
	 */
	public void setOutputCommitter(Class<? extends OutputCommitter> theClass) {
		setClass("mapred.output.committer.class", theClass,
				OutputCommitter.class);
	}

	/**
	 * Set the {@link OutputFormat} implementation for the map-reduce job.
	 * 
	 * @param theClass
	 *            the {@link OutputFormat} implementation for the map-reduce
	 *            job.
	 */
	public void setOutputFormat(Class<? extends OutputFormat> theClass) {
		setClass("mapred.output.format.class", theClass, OutputFormat.class);
	}

	/**
	 * Should the map outputs be compressed before transfer? Uses the
	 * SequenceFile compression.
	 * 
	 * @param compress
	 *            should the map outputs be compressed?
	 */
	public void setCompressMapOutput(boolean compress) {
		setBoolean("mapred.compress.map.output", compress);
	}

	/**
	 * Are the outputs of the maps be compressed?
	 * 
	 * @return <code>true</code> if the outputs of the maps are to be
	 *         compressed, <code>false</code> otherwise.
	 */
	public boolean getCompressMapOutput() {
		return getBoolean("mapred.compress.map.output", false);
	}

	/**
	 * Set the given class as the {@link CompressionCodec} for the map outputs.
	 * 
	 * @param codecClass
	 *            the {@link CompressionCodec} class that will compress the map
	 *            outputs.
	 */
	public void setMapOutputCompressorClass(
			Class<? extends CompressionCodec> codecClass) {
		setCompressMapOutput(true);
		setClass("mapred.map.output.compression.codec", codecClass,
				CompressionCodec.class);
	}

	/**
	 * Get the {@link CompressionCodec} for compressing the map outputs.
	 * 
	 * @param defaultValue
	 *            the {@link CompressionCodec} to return if not set
	 * @return the {@link CompressionCodec} class that should be used to
	 *         compress the map outputs.
	 * @throws IllegalArgumentException
	 *             if the class was specified, but not found
	 */
	public Class<? extends CompressionCodec> getMapOutputCompressorClass(
			Class<? extends CompressionCodec> defaultValue) {
		Class<? extends CompressionCodec> codecClass = defaultValue;
		String name = get("mapred.map.output.compression.codec");
		if (name != null) {
			try {
				codecClass = getClassByName(name).asSubclass(
						CompressionCodec.class);
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException("Compression codec " + name
						+ " was not found.", e);
			}
		}
		return codecClass;
	}

	/**
	 * Get the key class for the map output data. If it is not set, use the
	 * (final) output key class. This allows the map output key class to be
	 * different than the final output key class.
	 * 
	 * @return the map output key class.
	 */
	public Class<?> getMapOutputKeyClass() {
		Class<?> retv = getClass("mapred.mapoutput.key.class", null,
				Object.class);
		if (retv == null) {
			retv = getOutputKeyClass();
		}
		return retv;
	}

	/**
	 * Set the key class for the map output data. This allows the user to
	 * specify the map output key class to be different than the final output
	 * value class.
	 * 
	 * @param theClass
	 *            the map output key class.
	 */
	public void setMapOutputKeyClass(Class<?> theClass) {
		setClass("mapred.mapoutput.key.class", theClass, Object.class);
	}

	/**
	 * Get the value class for the map output data. If it is not set, use the
	 * (final) output value class This allows the map output value class to be
	 * different than the final output value class.
	 * 
	 * @return the map output value class.
	 */
	public Class<?> getMapOutputValueClass() {
		Class<?> retv = getClass("mapred.mapoutput.value.class", null,
				Object.class);
		if (retv == null) {
			retv = getOutputValueClass();
		}
		return retv;
	}

	/**
	 * Set the value class for the map output data. This allows the user to
	 * specify the map output value class to be different than the final output
	 * value class.
	 * 
	 * @param theClass
	 *            the map output value class.
	 */
	public void setMapOutputValueClass(Class<?> theClass) {
		setClass("mapred.mapoutput.value.class", theClass, Object.class);
	}

	/**
	 * Get the key class for the job output data.
	 * 
	 * @return the key class for the job output data.
	 */
	public Class<?> getOutputKeyClass() {
		return getClass("mapred.output.key.class", LongWritable.class,
				Object.class);
	}

	/**
	 * Set the key class for the job output data.
	 * 
	 * @param theClass
	 *            the key class for the job output data.
	 */
	public void setOutputKeyClass(Class<?> theClass) {
		setClass("mapred.output.key.class", theClass, Object.class);
	}

	/**
	 * Get the {@link RawComparator} comparator used to compare keys.
	 * 
	 * @return the {@link RawComparator} comparator used to compare keys.
	 */
	public RawComparator getOutputKeyComparator() {
		Class<? extends RawComparator> theClass = getClass(
				"mapred.output.key.comparator.class", null, RawComparator.class);
		if (theClass != null)
			return ReflectionUtils.newInstance(theClass, this);
		return WritableComparator.get(getMapOutputKeyClass().asSubclass(
				WritableComparable.class));
	}

	/**
	 * Set the {@link RawComparator} comparator used to compare keys.
	 * 
	 * @param theClass
	 *            the {@link RawComparator} comparator used to compare keys.
	 * @see #setOutputValueGroupingComparator(Class)
	 */
	public void setOutputKeyComparatorClass(
			Class<? extends RawComparator> theClass) {
		setClass("mapred.output.key.comparator.class", theClass,
				RawComparator.class);
	}

	/**
	 * Set the {@link KeyFieldBasedComparator} options used to compare keys.
	 * 
	 * @param keySpec
	 *            the key specification of the form -k pos1[,pos2], where, pos
	 *            is of the form f[.c][opts], where f is the number of the key
	 *            field to use, and c is the number of the first character from
	 *            the beginning of the field. Fields and character posns are
	 *            numbered starting with 1; a character position of zero in pos2
	 *            indicates the field's last character. If '.c' is omitted from
	 *            pos1, it defaults to 1 (the beginning of the field); if
	 *            omitted from pos2, it defaults to 0 (the end of the field).
	 *            opts are ordering options. The supported options are: -n,
	 *            (Sort numerically) -r, (Reverse the result of comparison)
	 */
	public void setKeyFieldComparatorOptions(String keySpec) {
		setOutputKeyComparatorClass(KeyFieldBasedComparator.class);
		set("mapred.text.key.comparator.options", keySpec);
	}

	/**
	 * Get the {@link KeyFieldBasedComparator} options
	 */
	public String getKeyFieldComparatorOption() {
		return get("mapred.text.key.comparator.options");
	}

	/**
	 * Set the {@link KeyFieldBasedPartitioner} options used for
	 * {@link Partitioner}
	 * 
	 * @param keySpec
	 *            the key specification of the form -k pos1[,pos2], where, pos
	 *            is of the form f[.c][opts], where f is the number of the key
	 *            field to use, and c is the number of the first character from
	 *            the beginning of the field. Fields and character posns are
	 *            numbered starting with 1; a character position of zero in pos2
	 *            indicates the field's last character. If '.c' is omitted from
	 *            pos1, it defaults to 1 (the beginning of the field); if
	 *            omitted from pos2, it defaults to 0 (the end of the field).
	 */
	public void setKeyFieldPartitionerOptions(String keySpec) {
		setPartitionerClass(KeyFieldBasedPartitioner.class);
		set("mapred.text.key.partitioner.options", keySpec);
	}

	/**
	 * Get the {@link KeyFieldBasedPartitioner} options
	 */
	public String getKeyFieldPartitionerOption() {
		return get("mapred.text.key.partitioner.options");
	}

	/**
	 * Get the user defined {@link WritableComparable} comparator for grouping
	 * keys of inputs to the reduce.
	 * 
	 * @return comparator set by the user for grouping values.
	 * @see #setOutputValueGroupingComparator(Class) for details.
	 */
	public RawComparator getOutputValueGroupingComparator() {
		Class<? extends RawComparator> theClass = getClass(
				"mapred.output.value.groupfn.class", null, RawComparator.class);
		if (theClass == null) {
			return getOutputKeyComparator();
		}

		return ReflectionUtils.newInstance(theClass, this);
	}

	/**
	 * Set the user defined {@link RawComparator} comparator for grouping keys
	 * in the input to the reduce.
	 * 
	 * <p>
	 * This comparator should be provided if the equivalence rules for keys for
	 * sorting the intermediates are different from those for grouping keys
	 * before each call to
	 * {@link Reducer#reduce(Object, java.util.Iterator, OutputCollector, Reporter)}
	 * .
	 * </p>
	 * 
	 * <p>
	 * For key-value pairs (K1,V1) and (K2,V2), the values (V1, V2) are passed
	 * in a single call to the reduce function if K1 and K2 compare as equal.
	 * </p>
	 * 
	 * <p>
	 * Since {@link #setOutputKeyComparatorClass(Class)} can be used to control
	 * how keys are sorted, this can be used in conjunction to simulate
	 * <i>secondary sort on values</i>.
	 * </p>
	 * 
	 * <p>
	 * <i>Note</i>: This is not a guarantee of the reduce sort being
	 * <i>stable</i> in any sense. (In any case, with the order of available
	 * map-outputs to the reduce being non-deterministic, it wouldn't make that
	 * much sense.)
	 * </p>
	 * 
	 * @param theClass
	 *            the comparator class to be used for grouping keys. It should
	 *            implement <code>RawComparator</code>.
	 * @see #setOutputKeyComparatorClass(Class)
	 */
	public void setOutputValueGroupingComparator(
			Class<? extends RawComparator> theClass) {
		setClass("mapred.output.value.groupfn.class", theClass,
				RawComparator.class);
	}

	/**
	 * Should the framework use the new context-object code for running the
	 * mapper?
	 * 
	 * @return true, if the new api should be used
	 */
	public boolean getUseNewMapper() {
		return getBoolean("mapred.mapper.new-api", false);
	}

	/**
	 * Set whether the framework should use the new api for the mapper. This is
	 * the default for jobs submitted with the new Job api.
	 * 
	 * @param flag
	 *            true, if the new api should be used
	 */
	public void setUseNewMapper(boolean flag) {
		setBoolean("mapred.mapper.new-api", flag);
	}

	/**
	 * Should the framework use the new context-object code for running the
	 * reducer?
	 * 
	 * @return true, if the new api should be used
	 */
	public boolean getUseNewReducer() {
		return getBoolean("mapred.reducer.new-api", false);
	}

	/**
	 * Set whether the framework should use the new api for the reducer. This is
	 * the default for jobs submitted with the new Job api.
	 * 
	 * @param flag
	 *            true, if the new api should be used
	 */
	public void setUseNewReducer(boolean flag) {
		setBoolean("mapred.reducer.new-api", flag);
	}

	/**
	 * Get the value class for job outputs.
	 * 
	 * @return the value class for job outputs.
	 */
	public Class<?> getOutputValueClass() {
		return getClass("mapred.output.value.class", Text.class, Object.class);
	}

	/**
	 * Set the value class for job outputs.
	 * 
	 * @param theClass
	 *            the value class for job outputs.
	 */
	public void setOutputValueClass(Class<?> theClass) {
		setClass("mapred.output.value.class", theClass, Object.class);
	}

	/**
	 * Get the {@link Mapper} class for the job.
	 * 
	 * @return the {@link Mapper} class for the job.
	 */
	public Class<? extends Mapper> getMapperClass() {
		return getClass("mapred.mapper.class", IdentityMapper.class,
				Mapper.class);
	}

	/**
	 * Set the {@link Mapper} class for the job.
	 * 
	 * @param theClass
	 *            the {@link Mapper} class for the job.
	 */
	public void setMapperClass(Class<? extends Mapper> theClass) {
		setClass("mapred.mapper.class", theClass, Mapper.class);
	}

	/**
	 * Get the {@link MapRunnable} class for the job.
	 * 
	 * @return the {@link MapRunnable} class for the job.
	 */
	public Class<? extends MapRunnable> getMapRunnerClass() {
		return getClass("mapred.map.runner.class", MapRunner.class,
				MapRunnable.class);
	}

	/**
	 * Expert: Set the {@link MapRunnable} class for the job.
	 * 
	 * Typically used to exert greater control on {@link Mapper}s.
	 * 
	 * @param theClass
	 *            the {@link MapRunnable} class for the job.
	 */
	public void setMapRunnerClass(Class<? extends MapRunnable> theClass) {
		setClass("mapred.map.runner.class", theClass, MapRunnable.class);
	}

	/**
	 * Get the {@link Partitioner} used to partition {@link Mapper}-outputs to
	 * be sent to the {@link Reducer}s.
	 * 
	 * @return the {@link Partitioner} used to partition map-outputs.
	 */
	public Class<? extends Partitioner> getPartitionerClass() {
		return getClass("mapred.partitioner.class", HashPartitioner.class,
				Partitioner.class);
	}

	/**
	 * Set the {@link Partitioner} class used to partition {@link Mapper}
	 * -outputs to be sent to the {@link Reducer}s.
	 * 
	 * @param theClass
	 *            the {@link Partitioner} used to partition map-outputs.
	 */
	public void setPartitionerClass(Class<? extends Partitioner> theClass) {
		setClass("mapred.partitioner.class", theClass, Partitioner.class);
	}

	/**
	 * Get the {@link Reducer} class for the job.
	 * 
	 * @return the {@link Reducer} class for the job.
	 */
	public Class<? extends Reducer> getReducerClass() {
		return getClass("mapred.reducer.class", IdentityReducer.class,
				Reducer.class);
	}

	/**
	 * Set the {@link Reducer} class for the job.
	 * 
	 * @param theClass
	 *            the {@link Reducer} class for the job.
	 */
	public void setReducerClass(Class<? extends Reducer> theClass) {
		setClass("mapred.reducer.class", theClass, Reducer.class);
	}

	/**
	 * Get the user-defined <i>combiner</i> class used to combine map-outputs
	 * before being sent to the reducers. Typically the combiner is same as the
	 * the {@link Reducer} for the job i.e. {@link #getReducerClass()}.
	 * 
	 * @return the user-defined combiner class used to combine map-outputs.
	 */
	public Class<? extends Reducer> getCombinerClass() {
		return getClass("mapred.combiner.class", null, Reducer.class);
	}

	/**
	 * Set the user-defined <i>combiner</i> class used to combine map-outputs
	 * before being sent to the reducers.
	 * 
	 * <p>
	 * The combiner is an application-specified aggregation operation, which can
	 * help cut down the amount of data transferred between the {@link Mapper}
	 * and the {@link Reducer}, leading to better performance.
	 * </p>
	 * 
	 * <p>
	 * The framework may invoke the combiner 0, 1, or multiple times, in both
	 * the mapper and reducer tasks. In general, the combiner is called as the
	 * sort/merge result is written to disk. The combiner must:
	 * <ul>
	 * <li>be side-effect free</li>
	 * <li>have the same input and output key types and the same input and
	 * output value types</li>
	 * </ul>
	 * </p>
	 * 
	 * <p>
	 * Typically the combiner is same as the <code>Reducer</code> for the job
	 * i.e. {@link #setReducerClass(Class)}.
	 * </p>
	 * 
	 * @param theClass
	 *            the user-defined combiner class used to combine map-outputs.
	 */
	public void setCombinerClass(Class<? extends Reducer> theClass) {
		setClass("mapred.combiner.class", theClass, Reducer.class);
	}

	/**
	 * Should speculative execution be used for this job? Defaults to
	 * <code>true</code>.
	 * 
	 * @return <code>true</code> if speculative execution be used for this job,
	 *         <code>false</code> otherwise.
	 */
	public boolean getSpeculativeExecution() {
		return (getMapSpeculativeExecution() || getReduceSpeculativeExecution());
	}

	/**
	 * Turn speculative execution on or off for this job.
	 * 
	 * @param speculativeExecution
	 *            <code>true</code> if speculative execution should be turned
	 *            on, else <code>false</code>.
	 */
	public void setSpeculativeExecution(boolean speculativeExecution) {
		setMapSpeculativeExecution(speculativeExecution);
		setReduceSpeculativeExecution(speculativeExecution);
	}

	/**
	 * Should speculative execution be used for this job for map tasks? Defaults
	 * to <code>true</code>.
	 * 
	 * @return <code>true</code> if speculative execution be used for this job
	 *         for map tasks, <code>false</code> otherwise.
	 */
	public boolean getMapSpeculativeExecution() {
		return getBoolean("mapred.map.tasks.speculative.execution", true);
	}

	/**
	 * Turn speculative execution on or off for this job for map tasks.
	 * 
	 * @param speculativeExecution
	 *            <code>true</code> if speculative execution should be turned on
	 *            for map tasks, else <code>false</code>.
	 */
	public void setMapSpeculativeExecution(boolean speculativeExecution) {
		setBoolean("mapred.map.tasks.speculative.execution",
				speculativeExecution);
	}

	/**
	 * Should speculative execution be used for this job for reduce tasks?
	 * Defaults to <code>true</code>.
	 * 
	 * @return <code>true</code> if speculative execution be used for reduce
	 *         tasks for this job, <code>false</code> otherwise.
	 */
	public boolean getReduceSpeculativeExecution() {
		return getBoolean("mapred.reduce.tasks.speculative.execution", true);
	}

	/**
	 * Turn speculative execution on or off for this job for reduce tasks.
	 * 
	 * @param speculativeExecution
	 *            <code>true</code> if speculative execution should be turned on
	 *            for reduce tasks, else <code>false</code>.
	 */
	public void setReduceSpeculativeExecution(boolean speculativeExecution) {
		setBoolean("mapred.reduce.tasks.speculative.execution",
				speculativeExecution);
	}

	/**
	 * Get configured the number of reduce tasks for this job. Defaults to
	 * <code>1</code>.
	 * 
	 * @return the number of reduce tasks for this job.
	 */
	public int getNumMapTasks() {
		return getInt("mapred.map.tasks", 1);
	}

	public void setNumMapTasks(int n) {
		setInt("mapred.map.tasks", n);
	}

	/**
	 * Get configured the number of reduce tasks for this job. Defaults to
	 * <code>1</code>.
	 * 
	 * @return the number of reduce tasks for this job.
	 */
	public int getNumReduceTasks() {
		return getInt("mapred.reduce.tasks", 1);
	}

	public void setNumReduceTasks(int n) {
		setInt("mapred.reduce.tasks", n);
	}

	/**
	 * Get the configured number of maximum attempts that will be made to run a
	 * map task, as specified by the <code>mapred.map.max.attempts</code>
	 * property. If this property is not already set, the default is 4 attempts.
	 * 
	 * @return the max number of attempts per map task.
	 */
	public int getMaxMapAttempts() {
		return getInt("mapred.map.max.attempts", 4);
	}

	/**
	 * Expert: Set the number of maximum attempts that will be made to run a map
	 * task.
	 * 
	 * @param n
	 *            the number of attempts per map task.
	 */
	public void setMaxMapAttempts(int n) {
		setInt("mapred.map.max.attempts", n);
	}

	/**
	 * Get the configured number of maximum attempts that will be made to run a
	 * reduce task, as specified by the <code>mapred.reduce.max.attempts</code>
	 * property. If this property is not already set, the default is 4 attempts.
	 * 
	 * @return the max number of attempts per reduce task.
	 */
	public int getMaxReduceAttempts() {
		return getInt("mapred.reduce.max.attempts", 4);
	}

	/**
	 * Expert: Set the number of maximum attempts that will be made to run a
	 * reduce task.
	 * 
	 * @param n
	 *            the number of attempts per reduce task.
	 */
	public void setMaxReduceAttempts(int n) {
		setInt("mapred.reduce.max.attempts", n);
	}

	/**
	 * Get the user-specified job name. This is only used to identify the job to
	 * the user.
	 * 
	 * @return the job's name, defaulting to "".
	 */
	public String getJobName() {
		return get("mapred.job.name", "");
	}

	/**
	 * Set the user-specified job name.
	 * 
	 * @param name
	 *            the job's new name.
	 */
	public void setJobName(String name) {
		set("mapred.job.name", name);
	}

	/**
	 * Get the user-specified session identifier. The default is the empty
	 * string.
	 * 
	 * The session identifier is used to tag metric data that is reported to
	 * some performance metrics system via the org.apache.hadoop.metrics API.
	 * The session identifier is intended, in particular, for use by
	 * Hadoop-On-Demand (HOD) which allocates a virtual Hadoop cluster
	 * dynamically and transiently. HOD will set the session identifier by
	 * modifying the mapred-site.xml file before starting the cluster.
	 * 
	 * When not running under HOD, this identifer is expected to remain set to
	 * the empty string.
	 * 
	 * @return the session identifier, defaulting to "".
	 */
	public String getSessionId() {
		return get("session.id", "");
	}

	/**
	 * Set the user-specified session identifier.
	 * 
	 * @param sessionId
	 *            the new session id.
	 */
	public void setSessionId(String sessionId) {
		set("session.id", sessionId);
	}

	/**
	 * Set the maximum no. of failures of a given job per tasktracker. If the
	 * no. of task failures exceeds <code>noFailures</code>, the tasktracker is
	 * <i>blacklisted</i> for this job.
	 * 
	 * @param noFailures
	 *            maximum no. of failures of a given job per tasktracker.
	 */
	public void setMaxTaskFailuresPerTracker(int noFailures) {
		setInt("mapred.max.tracker.failures", noFailures);
	}

	/**
	 * Expert: Get the maximum no. of failures of a given job per tasktracker.
	 * If the no. of task failures exceeds this, the tasktracker is
	 * <i>blacklisted</i> for this job.
	 * 
	 * @return the maximum no. of failures of a given job per tasktracker.
	 */
	public int getMaxTaskFailuresPerTracker() {
		return getInt("mapred.max.tracker.failures", 4);
	}

	/**
	 * Get the maximum percentage of map tasks that can fail without the job
	 * being aborted.
	 * 
	 * Each map task is executed a minimum of {@link #getMaxMapAttempts()}
	 * attempts before being declared as <i>failed</i>.
	 * 
	 * Defaults to <code>zero</code>, i.e. <i>any</i> failed map-task results in
	 * the job being declared as {@link JobStatus#FAILED}.
	 * 
	 * @return the maximum percentage of map tasks that can fail without the job
	 *         being aborted.
	 */
	public int getMaxMapTaskFailuresPercent() {
		return getInt("mapred.max.map.failures.percent", 0);
	}

	/**
	 * Expert: Set the maximum percentage of map tasks that can fail without the
	 * job being aborted.
	 * 
	 * Each map task is executed a minimum of {@link #getMaxMapAttempts}
	 * attempts before being declared as <i>failed</i>.
	 * 
	 * @param percent
	 *            the maximum percentage of map tasks that can fail without the
	 *            job being aborted.
	 */
	public void setMaxMapTaskFailuresPercent(int percent) {
		setInt("mapred.max.map.failures.percent", percent);
	}

	/**
	 * Get the maximum percentage of reduce tasks that can fail without the job
	 * being aborted.
	 * 
	 * Each reduce task is executed a minimum of {@link #getMaxReduceAttempts()}
	 * attempts before being declared as <i>failed</i>.
	 * 
	 * Defaults to <code>zero</code>, i.e. <i>any</i> failed reduce-task results
	 * in the job being declared as {@link JobStatus#FAILED}.
	 * 
	 * @return the maximum percentage of reduce tasks that can fail without the
	 *         job being aborted.
	 */
	public int getMaxReduceTaskFailuresPercent() {
		return getInt("mapred.max.reduce.failures.percent", 0);
	}

	/**
	 * Set the maximum percentage of reduce tasks that can fail without the job
	 * being aborted.
	 * 
	 * Each reduce task is executed a minimum of {@link #getMaxReduceAttempts()}
	 * attempts before being declared as <i>failed</i>.
	 * 
	 * @param percent
	 *            the maximum percentage of reduce tasks that can fail without
	 *            the job being aborted.
	 */
	public void setMaxReduceTaskFailuresPercent(int percent) {
		setInt("mapred.max.reduce.failures.percent", percent);
	}

	/**
	 * Set {@link JobPriority} for this job.
	 * 
	 * @param prio
	 *            the {@link JobPriority} for this job.
	 */
	public void setJobPriority(JobPriority prio) {
		set("mapred.job.priority", prio.toString());
	}

	/**
	 * Get the {@link JobPriority} for this job.
	 * 
	 * @return the {@link JobPriority} for this job.
	 */
	public JobPriority getJobPriority() {
		String prio = get("mapred.job.priority");
		if (prio == null) {
			return JobPriority.NORMAL;
		}

		return JobPriority.valueOf(prio);
	}

	/**
	 * Get whether the task profiling is enabled.
	 * 
	 * @return true if some tasks will be profiled
	 */
	public boolean getProfileEnabled() {
		return getBoolean("mapred.task.profile", false);
	}

	/**
	 * Set whether the system should collect profiler information for some of
	 * the tasks in this job? The information is stored in the user log
	 * directory.
	 * 
	 * @param newValue
	 *            true means it should be gathered
	 */
	public void setProfileEnabled(boolean newValue) {
		setBoolean("mapred.task.profile", newValue);
	}

	/**
	 * Get the profiler configuration arguments.
	 * 
	 * The default value for this property is
	 * "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s"
	 * 
	 * @return the parameters to pass to the task child to configure profiling
	 */
	public String getProfileParams() {
		return get("mapred.task.profile.params",
				"-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,"
						+ "verbose=n,file=%s");
	}

	/**
	 * Set the profiler configuration arguments. If the string contains a '%s'
	 * it will be replaced with the name of the profiling output file when the
	 * task runs.
	 * 
	 * This value is passed to the task child JVM on the command line.
	 * 
	 * @param value
	 *            the configuration string
	 */
	public void setProfileParams(String value) {
		set("mapred.task.profile.params", value);
	}

	/**
	 * Get the range of maps or reduces to profile.
	 * 
	 * @param isMap
	 *            is the task a map?
	 * @return the task ranges
	 */
	public IntegerRanges getProfileTaskRange(boolean isMap) {
		return getRange((isMap ? "mapred.task.profile.maps"
				: "mapred.task.profile.reduces"), "0-2");
	}

	/**
	 * Set the ranges of maps or reduces to profile. setProfileEnabled(true)
	 * must also be called.
	 * 
	 * @param newValue
	 *            a set of integer ranges of the map ids
	 */
	public void setProfileTaskRange(boolean isMap, String newValue) {
		// parse the value to make sure it is legal
		new Configuration.IntegerRanges(newValue);
		set((isMap ? "mapred.task.profile.maps" : "mapred.task.profile.reduces"),
				newValue);
	}

	/**
	 * Set the debug script to run when the map tasks fail.
	 * 
	 * <p>
	 * The debug script can aid debugging of failed map tasks. The script is
	 * given task's stdout, stderr, syslog, jobconf files as arguments.
	 * </p>
	 * 
	 * <p>
	 * The debug command, run on the node where the map failed, is:
	 * </p>
	 * <p>
	 * 
	 * <pre>
	 * <blockquote> 
	 * $script $stdout $stderr $syslog $jobconf.
	 * </blockquote>
	 * </pre>
	 * 
	 * </p>
	 * 
	 * <p>
	 * The script file is distributed through {@link DistributedCache} APIs. The
	 * script needs to be symlinked.
	 * </p>
	 * 
	 * <p>
	 * Here is an example on how to submit a script
	 * <p>
	 * <blockquote>
	 * 
	 * <pre>
	 * job.setMapDebugScript(&quot;./myscript&quot;);
	 * DistributedCache.createSymlink(job);
	 * DistributedCache.addCacheFile(&quot;/debug/scripts/myscript#myscript&quot;);
	 * </pre>
	 * 
	 * </blockquote>
	 * </p>
	 * 
	 * @param mDbgScript
	 *            the script name
	 */
	public void setMapDebugScript(String mDbgScript) {
		set("mapred.map.task.debug.script", mDbgScript);
	}

	/**
	 * Get the map task's debug script.
	 * 
	 * @return the debug Script for the mapred job for failed map tasks.
	 * @see #setMapDebugScript(String)
	 */
	public String getMapDebugScript() {
		return get("mapred.map.task.debug.script");
	}

	/**
	 * Set the debug script to run when the reduce tasks fail.
	 * 
	 * <p>
	 * The debug script can aid debugging of failed reduce tasks. The script is
	 * given task's stdout, stderr, syslog, jobconf files as arguments.
	 * </p>
	 * 
	 * <p>
	 * The debug command, run on the node where the map failed, is:
	 * </p>
	 * <p>
	 * 
	 * <pre>
	 * <blockquote> 
	 * $script $stdout $stderr $syslog $jobconf.
	 * </blockquote>
	 * </pre>
	 * 
	 * </p>
	 * 
	 * <p>
	 * The script file is distributed through {@link DistributedCache} APIs. The
	 * script file needs to be symlinked
	 * </p>
	 * 
	 * <p>
	 * Here is an example on how to submit a script
	 * <p>
	 * <blockquote>
	 * 
	 * <pre>
	 * job.setReduceDebugScript(&quot;./myscript&quot;);
	 * DistributedCache.createSymlink(job);
	 * DistributedCache.addCacheFile(&quot;/debug/scripts/myscript#myscript&quot;);
	 * </pre>
	 * 
	 * </blockquote>
	 * </p>
	 * 
	 * @param rDbgScript
	 *            the script name
	 */
	public void setReduceDebugScript(String rDbgScript) {
		set("mapred.reduce.task.debug.script", rDbgScript);
	}

	/**
	 * Get the reduce task's debug Script
	 * 
	 * @return the debug script for the mapred job for failed reduce tasks.
	 * @see #setReduceDebugScript(String)
	 */
	public String getReduceDebugScript() {
		return get("mapred.reduce.task.debug.script");
	}

	/**
	 * Get the uri to be invoked in-order to send a notification after the job
	 * has completed (success/failure).
	 * 
	 * @return the job end notification uri, <code>null</code> if it hasn't been
	 *         set.
	 * @see #setJobEndNotificationURI(String)
	 */
	public String getJobEndNotificationURI() {
		return get("job.end.notification.url");
	}

	/**
	 * Set the uri to be invoked in-order to send a notification after the job
	 * has completed (success/failure).
	 * 
	 * <p>
	 * The uri can contain 2 special parameters: <tt>$jobId</tt> and
	 * <tt>$jobStatus</tt>. Those, if present, are replaced by the job's
	 * identifier and completion-status respectively.
	 * </p>
	 * 
	 * <p>
	 * This is typically used by application-writers to implement chaining of
	 * Map-Reduce jobs in an <i>asynchronous manner</i>.
	 * </p>
	 * 
	 * @param uri
	 *            the job end notification uri
	 * @see JobStatus
	 * @see <a href="{@docRoot} /org/apache/hadoop/mapred/JobClient.html#
	 *      JobCompletionAndChaining">Job Completion and Chaining</a>
	 */
	public void setJobEndNotificationURI(String uri) {
		set("job.end.notification.url", uri);
	}

	/**
	 * Get job-specific shared directory for use as scratch space
	 * 
	 * <p>
	 * When a job starts, a shared directory is created at location <code>
	 * ${mapred.local.dir}/taskTracker/jobcache/$jobid/work/ </code> . This
	 * directory is exposed to the users through <code>job.local.dir </code>.
	 * So, the tasks can use this space as scratch space and share files among
	 * them.
	 * </p>
	 * This value is available as System property also.
	 * 
	 * @return The localized job specific shared directory
	 */
	public String getJobLocalDir() {
		return get("job.local.dir");
	}

	public long getMemoryForMapTask() {
		if (get(MAPRED_TASK_MAXVMEM_PROPERTY) != null) {
			long val = getLong(MAPRED_TASK_MAXVMEM_PROPERTY,
					DISABLED_MEMORY_LIMIT);
			return (val == DISABLED_MEMORY_LIMIT) ? val
					: ((val < 0) ? DISABLED_MEMORY_LIMIT : val / (1024 * 1024));
		}
		return getLong(JobConf.MAPRED_JOB_MAP_MEMORY_MB_PROPERTY,
				DISABLED_MEMORY_LIMIT);
	}

	public void setMemoryForMapTask(long mem) {
		setLong(JobConf.MAPRED_JOB_MAP_MEMORY_MB_PROPERTY, mem);
	}

	public long getMemoryForReduceTask() {
		if (get(MAPRED_TASK_MAXVMEM_PROPERTY) != null) {
			long val = getLong(MAPRED_TASK_MAXVMEM_PROPERTY,
					DISABLED_MEMORY_LIMIT);
			return (val == DISABLED_MEMORY_LIMIT) ? val
					: ((val < 0) ? DISABLED_MEMORY_LIMIT : val / (1024 * 1024));
		}
		return getLong(JobConf.MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY,
				DISABLED_MEMORY_LIMIT);
	}

	public void setMemoryForReduceTask(long mem) {
		setLong(JobConf.MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY, mem);
	}

	/**
	 * Return the name of the queue to which this job is submitted. Defaults to
	 * 'default'.
	 * 
	 * @return name of the queue
	 */
	public String getQueueName() {
		return get("mapred.job.queue.name", DEFAULT_QUEUE_NAME);
	}

	/**
	 * Set the name of the queue to which this job should be submitted.
	 * 
	 * @param queueName
	 *            Name of the queue
	 */
	public void setQueueName(String queueName) {
		set("mapred.job.queue.name", queueName);
	}

	/**
	 * Normalize the negative values in configuration
	 * 
	 * @param val
	 * @return normalized value
	 */
	public static long normalizeMemoryConfigValue(long val) {
		if (val < 0) {
			val = DISABLED_MEMORY_LIMIT;
		}
		return val;
	}

	/**
	 * Find a jar that contains a class of the same name, if any. It will return
	 * a jar file, even if that is not the first thing on the class path that
	 * has a class with the same name.
	 * 
	 * @param my_class
	 *            the class to find.
	 * @return a jar file that contains the class, or null.
	 * @throws IOException
	 */
	private static String findContainingJar(Class my_class) {
		ClassLoader loader = my_class.getClassLoader();
		String class_file = my_class.getName().replaceAll("\\.", "/")
				+ ".class";
		try {
			for (Enumeration itr = loader.getResources(class_file); itr
					.hasMoreElements();) {
				URL url = (URL) itr.nextElement();
				if ("jar".equals(url.getProtocol())) {
					String toReturn = url.getPath();
					if (toReturn.startsWith("file:")) {
						toReturn = toReturn.substring("file:".length());
					}
					toReturn = URLDecoder.decode(toReturn, "UTF-8");
					return toReturn.replaceAll("!.*$", "");
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	/**
	 * The maximum amount of memory any task of this job will use. See
	 * {@link #MAPRED_TASK_MAXVMEM_PROPERTY}
	 * <p/>
	 * mapred.task.maxvmem is split into mapred.job.map.memory.mb and
	 * mapred.job.map.memory.mb,mapred each of the new key are set as
	 * mapred.task.maxvmem / 1024 as new values are in MB
	 * 
	 * @return The maximum amount of memory any task of this job will use, in
	 *         bytes.
	 * @see #setMaxVirtualMemoryForTask(long)
	 * @deprecated Use {@link #getMemoryForMapTask()} and
	 *             {@link #getMemoryForReduceTask()}
	 */
	@Deprecated
	public long getMaxVirtualMemoryForTask() {
		LOG.warn("getMaxVirtualMemoryForTask() is deprecated. "
				+ "Instead use getMemoryForMapTask() and getMemoryForReduceTask()");

		if (get(JobConf.MAPRED_TASK_MAXVMEM_PROPERTY) == null) {
			if (get(JobConf.MAPRED_JOB_MAP_MEMORY_MB_PROPERTY) != null
					|| get(JobConf.MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY) != null) {
				long val = Math.max(getMemoryForMapTask(),
						getMemoryForReduceTask());
				if (val == JobConf.DISABLED_MEMORY_LIMIT) {
					return val;
				} else {
					if (val < 0) {
						return JobConf.DISABLED_MEMORY_LIMIT;
					}
					return val * 1024 * 1024;
					// Convert MB to byte as new value is in
					// MB and old deprecated method returns bytes
				}
			}
		}

		return getLong(JobConf.MAPRED_TASK_MAXVMEM_PROPERTY,
				DISABLED_MEMORY_LIMIT);
	}

	/**
	 * Set the maximum amount of memory any task of this job can use. See
	 * {@link #MAPRED_TASK_MAXVMEM_PROPERTY}
	 * <p/>
	 * mapred.task.maxvmem is split into mapred.job.map.memory.mb and
	 * mapred.job.map.memory.mb,mapred each of the new key are set as
	 * mapred.task.maxvmem / 1024 as new values are in MB
	 * 
	 * @param vmem
	 *            Maximum amount of virtual memory in bytes any task of this job
	 *            can use.
	 * @see #getMaxVirtualMemoryForTask()
	 * @deprecated Use {@link #setMemoryForMapTask(long mem)} and Use
	 *             {@link #setMemoryForReduceTask(long mem)}
	 */
	@Deprecated
	public void setMaxVirtualMemoryForTask(long vmem) {
		LOG.warn("setMaxVirtualMemoryForTask() is deprecated."
				+ "Instead use setMemoryForMapTask() and setMemoryForReduceTask()");
		if (vmem != DISABLED_MEMORY_LIMIT && vmem < 0) {
			setMemoryForMapTask(DISABLED_MEMORY_LIMIT);
			setMemoryForReduceTask(DISABLED_MEMORY_LIMIT);
		}

		if (get(JobConf.MAPRED_TASK_MAXVMEM_PROPERTY) == null) {
			setMemoryForMapTask(vmem / (1024 * 1024)); // Changing bytes to mb
			setMemoryForReduceTask(vmem / (1024 * 1024));// Changing bytes to mb
		} else {
			this.setLong(JobConf.MAPRED_TASK_MAXVMEM_PROPERTY, vmem);
		}
	}

	/**
	 * @deprecated this variable is deprecated and nolonger in use.
	 */
	@Deprecated
	public long getMaxPhysicalMemoryForTask() {
		LOG.warn("The API getMaxPhysicalMemoryForTask() is deprecated."
				+ " Refer to the APIs getMemoryForMapTask() and"
				+ " getMemoryForReduceTask() for details.");
		return -1;
	}

	/*
	 * @deprecated this
	 */
	@Deprecated
	public void setMaxPhysicalMemoryForTask(long mem) {
		LOG.warn("The API setMaxPhysicalMemoryForTask() is deprecated."
				+ " The value set is ignored. Refer to "
				+ " setMemoryForMapTask() and setMemoryForReduceTask() for details.");
	}

	static String deprecatedString(String key) {
		return "The variable " + key + " is no longer used.";
	}

	private void checkAndWarnDeprecation() {
		if (get(JobConf.MAPRED_TASK_MAXVMEM_PROPERTY) != null) {
			LOG.warn(JobConf
					.deprecatedString(JobConf.MAPRED_TASK_MAXVMEM_PROPERTY)
					+ " Instead use "
					+ JobConf.MAPRED_JOB_MAP_MEMORY_MB_PROPERTY
					+ " and "
					+ JobConf.MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY);
		}
	}

}
