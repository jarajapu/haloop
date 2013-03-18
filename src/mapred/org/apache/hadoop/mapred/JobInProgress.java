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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobHistory.Values;
import org.apache.hadoop.mapred.iterative.IFixPointChecker;
import org.apache.hadoop.mapred.iterative.LoopInputOutput;
import org.apache.hadoop.mapred.iterative.LoopMapCacheSwitch;
import org.apache.hadoop.mapred.iterative.LoopReduceCacheSwitch;
import org.apache.hadoop.mapred.iterative.LoopReduceOutputCacheSwitch;
import org.apache.hadoop.mapred.iterative.LoopStepHook;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/*************************************************************
 * JobInProgress maintains all the info for keeping a Job on the straight and
 * narrow. It keeps its JobProfile and its latest JobStatus, plus a set of
 * tables for doing bookkeeping of its Tasks.
 * ***********************************************************
 */
class JobInProgress {
	/**
	 * Used when the a kill is issued to a job which is initializing.
	 */
	static class KillInterruptedException extends InterruptedException {
		private static final long serialVersionUID = 1L;

		public KillInterruptedException(String msg) {
			super(msg);
		}
	}

	static final Log LOG = LogFactory.getLog(JobInProgress.class);

	/**
	 * the map task tracker list
	 */
	private List<TaskTrackerStatus> mapTaskTrackers = new ArrayList<TaskTrackerStatus>();

	/**
	 * the reduce task tracker list
	 */
	private List<TaskTrackerStatus> reduceTaskTrackers = new ArrayList<TaskTrackerStatus>();

	/**
	 * HaLoop: the variable indicates current iteration number
	 */
	int currentRounds = 0;

	/**
	 * HaLoop: the variable indicates specified iteration number
	 */
	int specifiedRounds = 0;

	/**
	 * HaLoop: specified step
	 */
	int specifiedSteps = 0;

	/**
	 * HaLoop: the variable indicates whether the iterative computation could
	 * exist
	 */
	boolean canExist = false;

	JobProfile profile;
	JobStatus status;
	Path jobFile = null;
	Path localJobFile = null;
	Path localJarFile = null;

	TaskInProgress maps[] = new TaskInProgress[0];
	TaskInProgress reduces[] = new TaskInProgress[0];
	TaskInProgress cleanup[] = new TaskInProgress[0];
	TaskInProgress setup[] = new TaskInProgress[0];
	int numMapTasks = 0;
	int numReduceTasks = 0;

	// Counters to track currently running/finished/failed Map/Reduce
	// task-attempts
	int runningMapTasks = 0;
	int runningReduceTasks = 0;
	int finishedMapTasks = 0;
	int finishedReduceTasks = 0;
	int failedMapTasks = 0;
	int failedReduceTasks = 0;

	private static float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
	int completedMapsForReduceSlowstart = 0;

	// runningMapTasks include speculative tasks, so we need to capture
	// speculative tasks separately
	int speculativeMapTasks = 0;
	int speculativeReduceTasks = 0;

	int mapFailuresPercent = 0;
	int reduceFailuresPercent = 0;
	int failedMapTIPs = 0;
	int failedReduceTIPs = 0;
	private volatile boolean launchedCleanup = false;
	private volatile boolean launchedSetup = false;
	private volatile boolean jobKilled = false;
	private volatile boolean jobFailed = false;

	JobPriority priority = JobPriority.NORMAL;
	final JobTracker jobtracker;

	// NetworkTopology Node to the set of TIPs
	Map<Node, List<TaskInProgress>> nonRunningMapCache;

	// Map of NetworkTopology Node to set of running TIPs
	Map<Node, Set<TaskInProgress>> runningMapCache;

	// A list of non-local non-running maps
	List<TaskInProgress> nonLocalMaps;

	// A set of non-local running maps
	Set<TaskInProgress> nonLocalRunningMaps;

	// A list of non-running reduce TIPs
	List<TaskInProgress> nonRunningReduces;

	// A set of running reduce TIPs
	Set<TaskInProgress> runningReduces;

	// A list of cleanup tasks for the map task attempts, to be launched
	List<TaskAttemptID> mapCleanupTasks = new LinkedList<TaskAttemptID>();

	// A list of cleanup tasks for the reduce task attempts, to be launched
	List<TaskAttemptID> reduceCleanupTasks = new LinkedList<TaskAttemptID>();

	/**
	 * HaLoop: hash map from task tracker to task attempt ID
	 */
	HashMap<String, String> taskToTrackername = new HashMap<String, String>();

	/**
	 * HaLoop: hash map from task tracker to map task ID
	 */
	HashMap<String, List<Integer>> taskTrackerToMapTask = new HashMap<String, List<Integer>>();

	/**
	 * HaLoop: hash map from task tracker to reduce task ID
	 */
	HashMap<String, List<Integer>> taskTrackerToReduceTask = new HashMap<String, List<Integer>>();

	/**
	 * HaLoop: indicate whether map is scheduled
	 */
	boolean mapScheduled[];

	/**
	 * HaLoop: indicate whether reduce is scheduled
	 */
	boolean reduceScheduled[];

	/**
	 * speculative bit
	 */
	boolean isSpeculativeMap[];// = false;

	/**
	 * speculative bit
	 */
	boolean isSpeculativeReduce[];// = false;

	/**
	 * HaLoop: scheduled iteration, iteration 0 by default is scheduled
	 */
	int scheduledIteration = 0;

	/**
	 * HaLoop: loop input/output structure
	 */
	private LoopInputOutput loopInputOutput = null;

	/**
	 * HaLoop: loop reduce cache switch
	 */
	private LoopReduceCacheSwitch loopReduceCacheSwitch = null;

	/**
	 * HaLoop: loop map cache switch
	 */
	private LoopMapCacheSwitch loopMapCacheSwitch = null;

	/**
	 * HaLoop: loop reduce output cache switch
	 */
	private LoopReduceOutputCacheSwitch loopReduceOutputCacheSwitch = null;

	/**
	 * HaLoop: fixpoint checker
	 */
	IFixPointChecker checker = null;

	/**
	 * HaLoop: loop step hook
	 */
	private LoopStepHook loopStepHook = null;

	/**
	 * HaLoop: scheduling log, note: currently only latest map history is
	 * maintained
	 */
	private FSDataOutputStream scheduleLog;

	/**
	 * HaLoop: from reduce partition to task tracker
	 */
	private HashMap<Integer, String> reducePartitionToTts = new HashMap<Integer, String>();

	private final int maxLevel;

	/**
	 * A special value indicating that
	 * {@link #findNewMapTask(TaskTrackerStatus, int, int, int, double)} should
	 * schedule any available map tasks for this job, including speculative
	 * tasks.
	 */
	private final int anyCacheLevel;

	/**
	 * A special value indicating that
	 * {@link #findNewMapTask(TaskTrackerStatus, int, int, int, double)} should
	 * schedule any only off-switch and speculative map tasks for this job.
	 */
	private static final int NON_LOCAL_CACHE_LEVEL = -1;

	private int taskCompletionEventTracker = 0;
	List<TaskCompletionEvent> taskCompletionEvents;

	// The maximum percentage of trackers in cluster added to the 'blacklist'.
	private static final double CLUSTER_BLACKLIST_PERCENT = 0.25;

	// The maximum percentage of fetch failures allowed for a map
	private static final double MAX_ALLOWED_FETCH_FAILURES_PERCENT = 0.5;

	// No. of tasktrackers in the cluster
	private volatile int clusterSize = 0;

	// The no. of tasktrackers where >= conf.getMaxTaskFailuresPerTracker()
	// tasks have failed
	private volatile int flakyTaskTrackers = 0;
	// Map of trackerHostName -> no. of task failures
	private Map<String, Integer> trackerToFailuresMap = new TreeMap<String, Integer>();

	// Confine estimation algorithms to an "oracle" class that JIP queries.
	private ResourceEstimator resourceEstimator;

	long startTime;
	long launchTime;
	long finishTime;

	// Indicates how many times the job got restarted
	private final int restartCount;

	private JobConf conf;
	AtomicBoolean tasksInited = new AtomicBoolean(false);
	private JobInitKillStatus jobInitKillStatus = new JobInitKillStatus();

	private LocalFileSystem localFs;
	private JobID jobId;
	private boolean hasSpeculativeMaps;
	private boolean hasSpeculativeReduces;
	private long inputLength = 0;

	private ClassLoader classLoader;

	private JobClient.RawSplit[] splitsCache = null;

	// Per-job counters
	public static enum Counter {
		NUM_FAILED_MAPS, NUM_FAILED_REDUCES, TOTAL_LAUNCHED_MAPS, TOTAL_LAUNCHED_REDUCES, OTHER_LOCAL_MAPS, DATA_LOCAL_MAPS, RACK_LOCAL_MAPS
	}

	private Counters jobCounters = new Counters();

	private MetricsRecord jobMetrics;

	// Maximum no. of fetch-failure notifications after which
	// the map task is killed
	private static final int MAX_FETCH_FAILURES_NOTIFICATIONS = 3;

	// Map of mapTaskId -> no. of fetch failures
	private Map<TaskAttemptID, Integer> mapTaskIdToFetchFailuresMap = new TreeMap<TaskAttemptID, Integer>();

	private Object schedulingInfo;

	/**
	 * Create an almost empty JobInProgress, which can be used only for tests
	 */
	protected JobInProgress(JobID jobid, JobConf conf) {
		this.conf = conf;
		this.jobId = jobid;
		this.numMapTasks = conf.getNumMapTasks();
		this.numReduceTasks = conf.getNumReduceTasks();
		this.maxLevel = NetworkTopology.DEFAULT_HOST_LEVEL;
		this.anyCacheLevel = this.maxLevel + 1;
		this.jobtracker = null;
		this.restartCount = 0;

		// note: there is a transformation between user-specified iterations
		// and job-in-progress internal iterations
		if (conf.isIterative()) {
			specifiedRounds = conf.getNumIterations()
					* conf.getNumberOfLoopBodySteps();
			specifiedSteps = conf.getNumberOfLoopBodySteps();
		}
	}

	/**
	 * HaLoop: refresh schedule log
	 * 
	 * @param conf
	 */
	private void writeJobConf() {
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(MRConstants.SCHEDULE_LOG_DIR + "/" + jobId
					+ "/conf.job");
			FSDataOutputStream out = fs.create(path);
			conf.write(out);
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * HaLoop: write job conf to hdfs
	 * 
	 * @param conf
	 */
	private void refreshScheduleLog(JobConf conf) {
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(MRConstants.SCHEDULE_LOG_DIR + "/" + jobId
					+ "/schedule.log");
			scheduleLog = fs.create(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// private List<MapScheduleInfo> msiList = new ArrayList<MapScheduleInfo>();

	/**
	 * HaLoop: add one row into the schedule history
	 */
	private synchronized void addScheduleHistory(String taskTracker, int i,
			TaskAttemptID tid) throws IOException {
		if (conf.isIterative()
				&& loopReduceCacheSwitch.isCacheWritten(conf,
						conf.getCurrentIteration(), conf.getCurrentStep())) {

			String port;
			if (conf.get("tasktracker.http.port") != null)
				port = conf.get("tasktracker.http.port");
			else {
				String httpAddress = conf
						.get("mapred.task.tracker.http.address");
				String[] ipAndPort = httpAddress.split(":");
				port = ipAndPort[1];
			}
			String entry = "http://" + taskTracker + ":" + port;
			// serialize task tracker & file split
			MapScheduleInfo msi = new MapScheduleInfo(entry, i, splitsCache[i],
					tid);
			msi.write(scheduleLog);
		}
	}

	/**
	 * Create a JobInProgress with the given job file, plus a handle to the
	 * tracker.
	 */
	public JobInProgress(JobID jobid, JobTracker jobtracker,
			JobConf default_conf) throws IOException {
		this(jobid, jobtracker, default_conf, 0);
	}

	public JobInProgress(JobID jobid, JobTracker jobtracker,
			JobConf default_conf, int rCount) throws IOException {
		this.restartCount = rCount;
		this.jobId = jobid;
		String url = "http://" + jobtracker.getJobTrackerMachine() + ":"
				+ jobtracker.getInfoPort() + "/jobdetails.jsp?jobid=" + jobid;
		this.jobtracker = jobtracker;
		this.status = new JobStatus(jobid, 0.0f, 0.0f, JobStatus.PREP);
		this.startTime = System.currentTimeMillis();
		status.setStartTime(startTime);
		this.localFs = FileSystem.getLocal(default_conf);

		JobConf default_job_conf = new JobConf(default_conf);
		this.localJobFile = default_job_conf.getLocalPath(JobTracker.SUBDIR
				+ "/" + jobid + ".xml");
		this.localJarFile = default_job_conf.getLocalPath(JobTracker.SUBDIR
				+ "/" + jobid + ".jar");
		Path jobDir = jobtracker.getSystemDirectoryForJob(jobId);
		FileSystem fs = jobDir.getFileSystem(default_conf);
		jobFile = new Path(jobDir, "job.xml");
		fs.copyToLocalFile(jobFile, localJobFile);
		conf = new JobConf(localJobFile);

		if (conf.isIterative() == true) {

			// load jobConf
			Path jobFileSer = new Path(jobDir, "jobser.txt");
			Path localJobFileSer = default_job_conf
					.getLocalPath(JobTracker.SUBDIR + "/" + jobid + "ser.txt");
			fs.copyToLocalFile(jobFileSer, localJobFileSer);
			conf = new JobConf();
			conf.readFields(fs.open(jobFileSer));
		}

		this.priority = conf.getJobPriority();
		this.status.setJobPriority(this.priority);
		this.profile = new JobProfile(conf.getUser(), jobid,
				jobFile.toString(), url, conf.getJobName(), conf.getQueueName());

		// set up classloader
		String jarFile = conf.getJar();
		if (jarFile != null) {
			fs.copyToLocalFile(new Path(jarFile), localJarFile);
			File file = new File(localJarFile.toString());
			URL jarURL = file.toURL();
			ClassLoader parent = this.getClass().getClassLoader();
			// create a new classloader with the old classloader as its parent
			// then set that classloader as the one used by the current class
			URL[] urls = new URL[1];
			urls[0] = jarURL;
			URLClassLoader loader = new URLClassLoader(urls, parent);

			// set current class loader to be the new one
			Thread.currentThread().setContextClassLoader(loader);
			classLoader = loader;
			conf.setClassLoader(loader);
		}

		this.numMapTasks = conf.getNumMapTasks();
		this.numReduceTasks = conf.getNumReduceTasks();
		this.taskCompletionEvents = new ArrayList<TaskCompletionEvent>(
				numMapTasks + numReduceTasks + 10);

		this.mapFailuresPercent = conf.getMaxMapTaskFailuresPercent();
		this.reduceFailuresPercent = conf.getMaxReduceTaskFailuresPercent();

		MetricsContext metricsContext = MetricsUtil.getContext("mapred");
		this.jobMetrics = MetricsUtil.createRecord(metricsContext, "job");
		this.jobMetrics.setTag("user", conf.getUser());
		this.jobMetrics.setTag("sessionId", conf.getSessionId());
		this.jobMetrics.setTag("jobName", conf.getJobName());
		this.jobMetrics.setTag("jobId", jobid.toString());
		hasSpeculativeMaps = conf.getMapSpeculativeExecution();
		hasSpeculativeReduces = conf.getReduceSpeculativeExecution();
		this.maxLevel = jobtracker.getNumTaskCacheLevels();
		this.anyCacheLevel = this.maxLevel + 1;
		this.nonLocalMaps = new LinkedList<TaskInProgress>();
		this.nonLocalRunningMaps = new LinkedHashSet<TaskInProgress>();
		this.runningMapCache = new IdentityHashMap<Node, Set<TaskInProgress>>();
		this.nonRunningReduces = new LinkedList<TaskInProgress>();
		this.runningReduces = new LinkedHashSet<TaskInProgress>();
		this.resourceEstimator = new ResourceEstimator(this);

		// note: see how specifiedIteration impact iteration task scheduling
		if (conf.isIterative()) {
			specifiedRounds = conf.getNumIterations()
					* conf.getNumberOfLoopBodySteps();
			specifiedSteps = conf.getNumberOfLoopBodySteps();
			System.out.println("number of iterations: "
					+ conf.getNumIterations());
			System.out.println("steps: " + conf.getNumberOfLoopBodySteps());
			System.out.println("init: specified iteration " + specifiedRounds);
		}

		// loop input
		loopInputOutput = ReflectionUtils.newInstance(
				conf.getLoopInputOutput(), conf);
		loopReduceCacheSwitch = ReflectionUtils.newInstance(
				conf.getLoopReduceCacheSwitch(), conf);
		loopMapCacheSwitch = ReflectionUtils.newInstance(
				conf.getLoopMapCacheSwitch(), conf);
		loopReduceOutputCacheSwitch = ReflectionUtils.newInstance(
				conf.getLoopReduceOutputCacheSwitch(), conf);
		checker = ReflectionUtils.newInstance(conf.getFixpointChecker(), conf);
		loopStepHook = ReflectionUtils
				.newInstance(conf.getLoopStepHook(), conf);

		if (conf.isIterative()) {
			writeJobConf();
		}

		this.isSpeculativeMap = new boolean[this.numMapTasks];
		this.isSpeculativeReduce = new boolean[this.numReduceTasks];
	}

	/**
	 * Called periodically by JobTrackerMetrics to update the metrics for this
	 * job.
	 */
	public void updateMetrics() {
		Counters counters = getCounters();
		for (Counters.Group group : counters) {
			jobMetrics.setTag("group", group.getDisplayName());
			for (Counters.Counter counter : group) {
				jobMetrics.setTag("counter", counter.getDisplayName());
				jobMetrics.setMetric("value", (float) counter.getCounter());
				jobMetrics.update();
			}
		}
	}

	/**
	 * Called when the job is complete
	 */
	public void cleanUpMetrics() {
		// Deletes all metric data for this job (in internal table in metrics
		// package).
		// This frees up RAM and possibly saves network bandwidth, since
		// otherwise
		// the metrics package implementation might continue to send these job
		// metrics
		// after the job has finished.
		jobMetrics.removeTag("group");
		jobMetrics.removeTag("counter");
		jobMetrics.remove();
	}

	private Map<Node, List<TaskInProgress>> createCache(
			JobClient.RawSplit[] splits, int maxLevel) {
		Map<Node, List<TaskInProgress>> cache = new IdentityHashMap<Node, List<TaskInProgress>>(
				maxLevel);

		for (int i = 0; i < splits.length; i++) {
			String[] splitLocations = splits[i].getLocations();
			if (splitLocations.length == 0) {
				nonLocalMaps.add(maps[i]);
				continue;
			}

			for (String host : splitLocations) {
				Node node = jobtracker.resolveAndAddToTopology(host);
				LOG.info("tip:" + maps[i].getTIPId() + " has split on node:"
						+ node);
				for (int j = 0; j < maxLevel; j++) {
					List<TaskInProgress> hostMaps = cache.get(node);
					if (hostMaps == null) {
						hostMaps = new ArrayList<TaskInProgress>();
						cache.put(node, hostMaps);
						hostMaps.add(maps[i]);
					}
					// check whether the hostMaps already contains an entry for
					// a TIP
					// This will be true for nodes that are racks and multiple
					// nodes in
					// the rack contain the input for a tip. Note that if it
					// already
					// exists in the hostMaps, it must be the last element there
					// since
					// we process one TIP at a time sequentially in the
					// split-size order
					if (hostMaps.get(hostMaps.size() - 1) != maps[i]) {
						hostMaps.add(maps[i]);
					}
					node = node.getParent();
				}
			}
		}
		return cache;
	}

	/**
	 * Check if the job has been initialized.
	 * 
	 * @return <code>true</code> if the job has been initialized,
	 *         <code>false</code> otherwise
	 */
	public boolean inited() {
		return tasksInited.get();
	}

	boolean hasRestarted() {
		return restartCount > 0;
	}

	/**
	 * Construct the splits, etc. This is invoked from an async thread so that
	 * split-computation doesn't block anyone.
	 */
	public synchronized void initTasks() throws IOException,
			KillInterruptedException {
		System.out.println("Yingyi: initiate tasks!");

		if (tasksInited.get() || isComplete()) {
			return;
		}
		synchronized (jobInitKillStatus) {
			if (jobInitKillStatus.killed || jobInitKillStatus.initStarted) {
				return;
			}
			jobInitKillStatus.initStarted = true;
		}

		LOG.info("Initializing " + jobId);

		// log job info
		JobHistory.JobInfo.logSubmitted(getJobID(), conf, jobFile.toString(),
				this.startTime, hasRestarted());
		// log the job priority
		setPriority(this.priority);

		//
		// read input splits and create a map per a split
		//
		String jobFile = profile.getJobFile();

		Path sysDir = new Path(this.jobtracker.getSystemDir());
		FileSystem fs = sysDir.getFileSystem(conf);
		DataInputStream splitFile = fs.open(new Path(conf
				.get("mapred.job.split.file")));
		JobClient.RawSplit[] splits;
		try {
			splits = JobClient.readSplitFile(splitFile);
		} finally {
			splitFile.close();
		}
		splitsCache = splits;
		numMapTasks = splits.length;

		// if the number of splits is larger than a configured value
		// then fail the job.
		int maxTasks = jobtracker.getMaxTasksPerJob();
		if (maxTasks > 0 && numMapTasks + numReduceTasks > maxTasks) {
			throw new IOException("The number of tasks for this job "
					+ (numMapTasks + numReduceTasks)
					+ " exceeds the configured limit " + maxTasks);
		}
		jobtracker.getInstrumentation().addWaiting(getJobID(),
				numMapTasks + numReduceTasks);

		if (conf.isIterative())
			loopStepHook.preStep(conf, conf.getCurrentIteration(),
					conf.getCurrentStep());
		/**
		 * HaLoop: logging scheduling information if reduce cache is written
		 */
		if (conf.isIterative()
				&& loopReduceCacheSwitch.isCacheWritten(conf,
						conf.getCurrentIteration(), conf.getCurrentStep()))
			refreshScheduleLog(conf);

		maps = new TaskInProgress[numMapTasks];
		for (int i = 0; i < numMapTasks; ++i) {
			inputLength += splits[i].getDataLength();
			maps[i] = new TaskInProgress(jobId, jobFile, splits[i], jobtracker,
					conf, this, i);
		}
		LOG.info("Input size for job " + jobId + " = " + inputLength
				+ ". Number of splits = " + splits.length);
		if (numMapTasks > 0) {
			nonRunningMapCache = createCache(splits, maxLevel);
		}

		// set the launch time
		this.launchTime = System.currentTimeMillis();

		//
		// Create reduce tasks
		//
		this.reduces = new TaskInProgress[numReduceTasks];
		for (int i = 0; i < numReduceTasks; i++) {
			reduces[i] = new TaskInProgress(jobId, jobFile, numMapTasks, i,
					jobtracker, conf, this);
			nonRunningReduces.add(reduces[i]);
		}

		// Calculate the minimum number of maps to be complete before
		// we should start scheduling reduces
		completedMapsForReduceSlowstart = (int) Math
				.ceil((conf.getFloat("mapred.reduce.slowstart.completed.maps",
						DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART) * numMapTasks));

		// create cleanup two cleanup tips, one map and one reduce.
		cleanup = new TaskInProgress[2];

		// cleanup map tip. This map doesn't use any splits. Just assign an
		// empty
		// split.
		JobClient.RawSplit emptySplit = new JobClient.RawSplit();
		cleanup[0] = new TaskInProgress(jobId, jobFile, emptySplit, jobtracker,
				conf, this, numMapTasks);
		cleanup[0].setJobCleanupTask();

		// cleanup reduce tip.
		cleanup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
				numReduceTasks, jobtracker, conf, this);
		cleanup[1].setJobCleanupTask();

		// create two setup tips, one map and one reduce.
		setup = new TaskInProgress[2];

		// setup map tip. This map doesn't use any split. Just assign an empty
		// split.
		setup[0] = new TaskInProgress(jobId, jobFile, emptySplit, jobtracker,
				conf, this, numMapTasks + 1);
		setup[0].setJobSetupTask();

		// setup reduce tip.
		setup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
				numReduceTasks + 1, jobtracker, conf, this);
		setup[1].setJobSetupTask();

		synchronized (jobInitKillStatus) {
			jobInitKillStatus.initDone = true;
			if (jobInitKillStatus.killed) {
				throw new KillInterruptedException("Job " + jobId
						+ " killed in init");
			}
		}

		tasksInited.set(true);
		JobHistory.JobInfo.logInited(profile.getJobID(), this.launchTime,
				numMapTasks, numReduceTasks);

		// set current iteration input/output
		if (conf.isIterative() == true) {
			List<String> inputPaths = loopInputOutput.getLoopInputs(conf,
					conf.getCurrentIteration(), conf.getCurrentStep());

			for (String path : inputPaths)
				System.out.println("path: " + path);

			if (inputPaths.size() > 0) {
				// FileInputFormat.
				FileInputFormat.setInputPaths(conf, inputPaths.get(0));
				for (int i = 1; i < inputPaths.size(); i++)
					FileInputFormat.addInputPaths(conf, inputPaths.get(i));
			}

			// set the output path for this step in this iteration
			String outputPath = loopInputOutput.getLoopOutputs(conf,
					conf.getCurrentIteration(), conf.getCurrentStep());
			FileOutputFormat.setOutputPath(conf, new Path(outputPath));
			// sSystem.out.println("move to pass " + currentIteration);
		}

	}

	/**
	 * HaLoop: initiate at each iteration re-enter tasks into the cache
	 */
	private synchronized void iterationInitTasks() throws IOException,
			KillInterruptedException {

		/**
		 * HaLoop: close sheduling log
		 */
		if (scheduleLog != null) {
			try {
				scheduleLog.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if (conf.isIterative()
				&& Thread.currentThread().getContextClassLoader() != classLoader) {
			Thread.currentThread().setContextClassLoader(classLoader);
			conf.setClassLoader(classLoader);
		}

		LOG.info("Initializing a new iteration for job: " + jobId);

		if (conf.isIterative() == true) {

			// move to next step/iteration
			conf.next();

			if (conf.isIterative())
				loopStepHook.preStep(conf, conf.getCurrentIteration(),
						conf.getCurrentStep());
			/**
			 * HaLoop: logging scheduling information if reduce cache is written
			 */
			// if (loopReduceCacheSwitch.isCacheWritten(conf, conf
			// .getCurrentIteration(), conf.getCurrentStep()))
			// refreshScheduleLog(conf);

			// set the input paths of next iteration
			List<String> inputPaths = loopInputOutput.getLoopInputs(conf,
					conf.getCurrentIteration(), conf.getCurrentStep());
			if (inputPaths.size() > 0) {
				// FileInputFormat.
				FileInputFormat.setInputPaths(conf, inputPaths.get(0));
				for (int i = 1; i < inputPaths.size(); i++)
					FileInputFormat.addInputPaths(conf, inputPaths.get(i));
			}

			// set the output path for this step in this iteration
			String outputPath = loopInputOutput.getLoopOutputs(conf,
					conf.getCurrentIteration(), conf.getCurrentStep());
			FileOutputFormat.setOutputPath(conf, new Path(outputPath));
			System.out.println("move to pass " + currentRounds);
		}

		// conf.setCurrentIteration(currentIteration);
		LOG.info(conf.get("mapred.input.dir"));

		JobClient jc = new JobClient(this.conf, this.jobId);

		try {
			System.out.println("mapper class before submission: "
					+ conf.getMapperClass().getName());
			System.out.println("reducer class before submission: "
					+ conf.getReducerClass().getName());

			Path[] inputPath = FileInputFormat.getInputPaths(conf);
			for (Path p : inputPath)
				System.out.println("input path before submission: " + p);
			jc.reSubmitJobInternal(jobtracker, conf, jobId);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// print(profile.getJobFile());

		this.numMapTasks = conf.getNumMapTasks();
		// conf.setNumReduceTasks(numMapTasks);
		this.numReduceTasks = conf.getNumReduceTasks();
		this.taskCompletionEvents = new ArrayList<TaskCompletionEvent>(
				numMapTasks + numReduceTasks + 10);

		if (isComplete()) {
			System.out.println("it is completed!");
			return;
		}
		synchronized (jobInitKillStatus) {
			if (jobInitKillStatus.killed) {
				System.out.println("it is killed!");
				return;
			}
			jobInitKillStatus.initStarted = true;
		}

		LOG.info("Initializing a new iteration for job: " + jobId);
		System.out.println("Initializing a new iteration for job: " + jobId);

		// HaLoop: clear all the states
		nonRunningMapCache.clear();
		runningMapCache.clear();
		nonRunningReduces.clear();
		runningReduces.clear();

		failedMapTasks = 0;
		failedMapTIPs = 0;
		failedReduceTasks = 0;
		failedReduceTIPs = 0;
		finishedReduceTasks = 0;
		finishedMapTasks = 0;

		nonLocalMaps.clear();
		nonLocalRunningMaps.clear();

		//
		// read input splits and create a map per a split
		//
		String jobFile = profile.getJobFile();

		System.out.println("job file to tasks: " + jobFile);

		Path sysDir = new Path(this.jobtracker.getSystemDir());
		FileSystem fs = sysDir.getFileSystem(conf);
		DataInputStream splitFile = fs.open(new Path(conf
				.get("mapred.job.split.file")));
		JobClient.RawSplit[] splits;

		try {
			splits = JobClient.readSplitFile(splitFile);
		} finally {
			splitFile.close();
		}
		if (conf.getMapperInputCacheOption() && splitsCache != null)
			splits = splitsCache;
		numMapTasks = splits.length;

		// if the number of splits is larger than a configured value
		// then fail the job.
		int maxTasks = jobtracker.getMaxTasksPerJob();
		if (maxTasks > 0 && numMapTasks + numReduceTasks > maxTasks) {
			throw new IOException("The number of tasks for this job "
					+ (numMapTasks + numReduceTasks)
					+ " exceeds the configured limit " + maxTasks);
		}
		jobtracker.getInstrumentation().addWaiting(getJobID(),
				numMapTasks + numReduceTasks);

		maps = new TaskInProgress[numMapTasks];
		for (int i = 0; i < numMapTasks; ++i) {
			inputLength += splits[i].getDataLength();
			maps[i] = new TaskInProgress(jobId, jobFile, splits[i], jobtracker,
					conf, this, i);
		}
		LOG.info("Iteration " + currentRounds + " Input size for job " + jobId
				+ " = " + inputLength + ". Number of splits = " + splits.length);

		if (numMapTasks > 0) {
			System.out.println("mapper num: " + numMapTasks
					+ " create cache for iteration: " + currentRounds);
			nonRunningMapCache = createCache(splits, maxLevel);
		}

		status.setMapProgress(0);
		status.setReduceProgress(0);

		//
		// Create reduce tasks
		//
		this.reduces = new TaskInProgress[numReduceTasks];
		for (int i = 0; i < numReduceTasks; i++) {
			reduces[i] = new TaskInProgress(jobId, jobFile, numMapTasks, i,
					jobtracker, conf, this);
			nonRunningReduces.add(reduces[i]);
		}

		// Calculate the minimum number of maps to be complete before
		// we should start scheduling reduces
		completedMapsForReduceSlowstart = (int) Math
				.ceil((conf.getFloat("mapred.reduce.slowstart.completed.maps",
						DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART) * numMapTasks));

		LOG.info("iteration " + currentRounds + " started!");
		System.out.println("iteration " + currentRounds + " started!");

		this.isSpeculativeMap = new boolean[this.numMapTasks];
		this.isSpeculativeReduce = new boolean[this.numReduceTasks];

		for (int i = 0; i < isSpeculativeMap.length; i++)
			isSpeculativeMap[i] = false;

		for (int i = 0; i < isSpeculativeReduce.length; i++)
			isSpeculativeReduce[i] = false;
	}

	/**
	 * check whether the mapper is scheduled
	 * 
	 * @param t
	 *            tasktracker
	 * @return true if the mapper is scheduled
	 */
	public boolean isMapScheduled(TaskTrackerStatus t) {
		for (int i = 0; i < mapTaskTrackers.size(); i++) {
			if (mapTaskTrackers.get(i).getTrackerName()
					.equals(t.getTrackerName()))
				return true;
		}

		return false;
	}

	public boolean isReduceScheduled(TaskTrackerStatus t) {
		for (int i = 0; i < reduceTaskTrackers.size(); i++) {
			if (reduceTaskTrackers.get(i).getTrackerName()
					.equals(t.getTrackerName()))
				return true;
		}

		return false;
	}

	public void addMap(TaskTrackerStatus t) {
		mapTaskTrackers.add(t);
	}

	// ///////////////////////////////////////////////////
	// Accessors for the JobInProgress
	// ///////////////////////////////////////////////////
	public JobProfile getProfile() {
		return profile;
	}

	public JobStatus getStatus() {
		return status;
	}

	public synchronized long getLaunchTime() {
		return launchTime;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getFinishTime() {
		return finishTime;
	}

	public int desiredMaps() {
		return numMapTasks;
	}

	public synchronized int finishedMaps() {
		return finishedMapTasks;
	}

	public int desiredReduces() {
		return numReduceTasks;
	}

	public synchronized int runningMaps() {
		return runningMapTasks;
	}

	public synchronized int runningReduces() {
		return runningReduceTasks;
	}

	public synchronized int finishedReduces() {
		return finishedReduceTasks;
	}

	public synchronized int pendingMaps() {
		return numMapTasks - runningMapTasks - failedMapTIPs - finishedMapTasks
				+ speculativeMapTasks;
	}

	public synchronized int pendingReduces() {
		return numReduceTasks - runningReduceTasks - failedReduceTIPs
				- finishedReduceTasks + speculativeReduceTasks;
	}

	public JobPriority getPriority() {
		return this.priority;
	}

	public void setPriority(JobPriority priority) {
		if (priority == null) {
			this.priority = JobPriority.NORMAL;
		} else {
			this.priority = priority;
		}
		synchronized (this) {
			status.setJobPriority(priority);
		}
		// log and change to the job's priority
		JobHistory.JobInfo.logJobPriority(jobId, priority);
	}

	// Update the job start/launch time (upon restart) and log to history
	synchronized void updateJobInfo(long startTime, long launchTime) {
		// log and change to the job's start/launch time
		this.startTime = startTime;
		this.launchTime = launchTime;
		JobHistory.JobInfo.logJobInfo(jobId, startTime, launchTime);
	}

	/**
	 * Get the number of times the job has restarted
	 */
	int getNumRestarts() {
		return restartCount;
	}

	long getInputLength() {
		return inputLength;
	}

	boolean isCleanupLaunched() {
		return launchedCleanup;
	}

	boolean isSetupLaunched() {
		return launchedSetup;
	}

	/**
	 * Get the list of map tasks
	 * 
	 * @return the raw array of maps for this job
	 */
	TaskInProgress[] getMapTasks() {
		return maps;
	}

	/**
	 * Get the list of cleanup tasks
	 * 
	 * @return the array of cleanup tasks for the job
	 */
	TaskInProgress[] getCleanupTasks() {
		return cleanup;
	}

	/**
	 * Get the list of setup tasks
	 * 
	 * @return the array of setup tasks for the job
	 */
	TaskInProgress[] getSetupTasks() {
		return setup;
	}

	/**
	 * Get the list of reduce tasks
	 * 
	 * @return the raw array of reduce tasks for this job
	 */
	TaskInProgress[] getReduceTasks() {
		return reduces;
	}

	/**
	 * Return the nonLocalRunningMaps
	 * 
	 * @return
	 */
	Set<TaskInProgress> getNonLocalRunningMaps() {
		return nonLocalRunningMaps;
	}

	/**
	 * Return the runningMapCache
	 * 
	 * @return
	 */
	Map<Node, Set<TaskInProgress>> getRunningMapCache() {
		return runningMapCache;
	}

	/**
	 * Return runningReduces
	 * 
	 * @return
	 */
	Set<TaskInProgress> getRunningReduces() {
		return runningReduces;
	}

	/**
	 * Get the job configuration
	 * 
	 * @return the job's configuration
	 */
	JobConf getJobConf() {
		return conf;
	}

	/**
	 * Return a vector of completed TaskInProgress objects
	 */
	public synchronized Vector<TaskInProgress> reportTasksInProgress(
			boolean shouldBeMap, boolean shouldBeComplete) {

		Vector<TaskInProgress> results = new Vector<TaskInProgress>();
		TaskInProgress tips[] = null;
		if (shouldBeMap) {
			tips = maps;
		} else {
			tips = reduces;
		}
		for (int i = 0; i < tips.length; i++) {
			if (tips[i].isComplete() == shouldBeComplete) {
				results.add(tips[i]);
			}
		}
		return results;
	}

	/**
	 * Return a vector of cleanup TaskInProgress objects
	 */
	public synchronized Vector<TaskInProgress> reportCleanupTIPs(
			boolean shouldBeComplete) {

		Vector<TaskInProgress> results = new Vector<TaskInProgress>();
		for (int i = 0; i < cleanup.length; i++) {
			if (cleanup[i].isComplete() == shouldBeComplete) {
				results.add(cleanup[i]);
			}
		}
		return results;
	}

	/**
	 * Return a vector of setup TaskInProgress objects
	 */
	public synchronized Vector<TaskInProgress> reportSetupTIPs(
			boolean shouldBeComplete) {

		Vector<TaskInProgress> results = new Vector<TaskInProgress>();
		for (int i = 0; i < setup.length; i++) {
			if (setup[i].isComplete() == shouldBeComplete) {
				results.add(setup[i]);
			}
		}
		return results;
	}

	// //////////////////////////////////////////////////
	// Status update methods
	// //////////////////////////////////////////////////

	/**
	 * Assuming {@link JobTracker} is locked on entry.
	 */
	public synchronized void updateTaskStatus(TaskInProgress tip,
			TaskStatus status) {

		double oldProgress = tip.getProgress(); // save old progress
		boolean wasRunning = tip.isRunning();
		boolean wasComplete = tip.isComplete();
		boolean wasPending = tip.isOnlyCommitPending();
		TaskAttemptID taskid = status.getTaskID();

		// If the TIP is already completed and the task reports as SUCCEEDED
		// then
		// mark the task as KILLED.
		// In case of task with no promotion the task tracker will mark the task
		// as SUCCEEDED.
		// User has requested to kill the task, but TT reported SUCCEEDED,
		// mark the task KILLED.
		if ((wasComplete || tip.wasKilled(taskid))
				&& (status.getRunState() == TaskStatus.State.SUCCEEDED)) {
			status.setRunState(TaskStatus.State.KILLED);
		}

		// If the job is complete and a task has just reported its
		// state as FAILED_UNCLEAN/KILLED_UNCLEAN,
		// make the task's state FAILED/KILLED without launching cleanup
		// attempt.
		// Note that if task is already a cleanup attempt,
		// we don't change the state to make sure the task gets a killTaskAction
		if ((this.isComplete() || jobFailed || jobKilled)
				&& !tip.isCleanupAttempt(taskid)) {
			if (status.getRunState() == TaskStatus.State.FAILED_UNCLEAN) {
				status.setRunState(TaskStatus.State.FAILED);
			} else if (status.getRunState() == TaskStatus.State.KILLED_UNCLEAN) {
				status.setRunState(TaskStatus.State.KILLED);
			}
		}

		boolean change = tip.updateStatus(status);
		if (change) {
			TaskStatus.State state = status.getRunState();
			// get the TaskTrackerStatus where the task ran
			TaskTrackerStatus ttStatus = this.jobtracker.getTaskTracker(tip
					.machineWhereTaskRan(taskid));
			String httpTaskLogLocation = null;

			if (null != ttStatus) {
				String host;
				if (NetUtils.getStaticResolution(ttStatus.getHost()) != null) {
					host = NetUtils.getStaticResolution(ttStatus.getHost());
				} else {
					host = ttStatus.getHost();
				}
				httpTaskLogLocation = "http://" + host + ":"
						+ ttStatus.getHttpPort();
				// + "/tasklog?plaintext=true&taskid=" + status.getTaskID();
			}

			TaskCompletionEvent taskEvent = null;
			if (state == TaskStatus.State.SUCCEEDED) {
				taskEvent = new TaskCompletionEvent(taskCompletionEventTracker,
						taskid, tip.idWithinJob(), status.getIsMap()
								&& !tip.isJobCleanupTask()
								&& !tip.isJobSetupTask(),
						TaskCompletionEvent.Status.SUCCEEDED,
						httpTaskLogLocation);
				taskEvent.setTaskRunTime((int) (status.getFinishTime() - status
						.getStartTime()));
				tip.setSuccessEventNumber(taskCompletionEventTracker);
			} else if (state == TaskStatus.State.COMMIT_PENDING) {
				// If it is the first attempt reporting COMMIT_PENDING
				// ask the task to commit.
				if (!wasComplete && !wasPending) {
					tip.doCommit(taskid);
				}
				return;
			} else if (state == TaskStatus.State.FAILED_UNCLEAN
					|| state == TaskStatus.State.KILLED_UNCLEAN) {
				tip.incompleteSubTask(taskid, this.status);
				// add this task, to be rescheduled as cleanup attempt
				if (tip.isMapTask()) {
					mapCleanupTasks.add(taskid);
				} else {
					reduceCleanupTasks.add(taskid);
				}
				// Remove the task entry from jobtracker
				jobtracker.removeTaskEntry(taskid);
			}
			// For a failed task update the JT datastructures.
			else if (state == TaskStatus.State.FAILED
					|| state == TaskStatus.State.KILLED) {
				// Get the event number for the (possibly) previously successful
				// task. If there exists one, then set that status to OBSOLETE
				int eventNumber;
				if ((eventNumber = tip.getSuccessEventNumber()) != -1) {

					/**
					 * HaLoop: temporary fix for the index-of-bound exception,
					 * related to Hadoop-1060
					 */

					try {
						TaskCompletionEvent t = this.taskCompletionEvents
								.get(eventNumber);
						if (t.getTaskAttemptId().equals(taskid))
							t.setTaskStatus(TaskCompletionEvent.Status.OBSOLETE);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				// Tell the job to fail the relevant task
				failedTask(tip, taskid, status, ttStatus, wasRunning,
						wasComplete);

				// Did the task failure lead to tip failure?
				TaskCompletionEvent.Status taskCompletionStatus = (state == TaskStatus.State.FAILED) ? TaskCompletionEvent.Status.FAILED
						: TaskCompletionEvent.Status.KILLED;
				if (tip.isFailed()) {
					taskCompletionStatus = TaskCompletionEvent.Status.TIPFAILED;
				}
				taskEvent = new TaskCompletionEvent(taskCompletionEventTracker,
						taskid, tip.idWithinJob(), status.getIsMap()
								&& !tip.isJobCleanupTask()
								&& !tip.isJobSetupTask(), taskCompletionStatus,
						httpTaskLogLocation);
			}

			// Add the 'complete' task i.e. successful/failed
			// It _is_ safe to add the TaskCompletionEvent.Status.SUCCEEDED
			// *before* calling TIP.completedTask since:
			// a. One and only one task of a TIP is declared as a SUCCESS, the
			// other (speculative tasks) are marked KILLED by the
			// TaskCommitThread
			// b. TIP.completedTask *does not* throw _any_ exception at all.
			if (taskEvent != null) {
				System.out.println(currentRounds + " add " + taskEvent
						+ " from " + taskEvent.getTaskTrackerHttp());
				this.taskCompletionEvents.add(taskEvent);
				taskCompletionEventTracker++;
				if (state == TaskStatus.State.SUCCEEDED) {
					completedTask(tip, status);
				}
			}
		}

		//
		// Update JobInProgress status
		//
		if (LOG.isDebugEnabled()) {
			LOG.debug("Taking progress for " + tip.getTIPId() + " from "
					+ oldProgress + " to " + tip.getProgress());
		}

		if (!tip.isJobCleanupTask() && !tip.isJobSetupTask()) {
			double progressDelta = tip.getProgress() - oldProgress;
			if (tip.isMapTask()) {
				this.status
						.setMapProgress((float) (this.status.mapProgress() + progressDelta
								/ maps.length));
			} else {
				this.status.setReduceProgress((float) (this.status
						.reduceProgress() + (progressDelta / reduces.length)));
			}
		}
	}

	/**
	 * Returns the job-level counters.
	 * 
	 * @return the job-level counters.
	 */
	public synchronized Counters getJobCounters() {
		return jobCounters;
	}

	/**
	 * Returns map phase counters by summing over all map tasks in progress.
	 */
	public synchronized Counters getMapCounters() {
		return incrementTaskCounters(new Counters(), maps);
	}

	/**
	 * Returns map phase counters by summing over all map tasks in progress.
	 */
	public synchronized Counters getReduceCounters() {
		return incrementTaskCounters(new Counters(), reduces);
	}

	/**
	 * Returns the total job counters, by adding together the job, the map and
	 * the reduce counters.
	 */
	public synchronized Counters getCounters() {
		Counters result = new Counters();
		result.incrAllCounters(getJobCounters());
		incrementTaskCounters(result, maps);
		return incrementTaskCounters(result, reduces);
	}

	/**
	 * Increments the counters with the counters from each task.
	 * 
	 * @param counters
	 *            the counters to increment
	 * @param tips
	 *            the tasks to add in to counters
	 * @return counters the same object passed in as counters
	 */
	private Counters incrementTaskCounters(Counters counters,
			TaskInProgress[] tips) {
		for (TaskInProgress tip : tips) {
			counters.incrAllCounters(tip.getCounters());
		}
		return counters;
	}

	// ///////////////////////////////////////////////////
	// Create/manage tasks
	// ///////////////////////////////////////////////////
	/**
	 * Return a MapTask, if appropriate, to run on the given tasktracker
	 */
	public synchronized Task obtainNewMapTask(TaskTrackerStatus tts,
			int clusterSize, int numUniqueHosts) throws IOException {
		if (status.getRunState() != JobStatus.RUNNING) {
			LOG.info("Cannot create task split for " + profile.getJobID());
			return null;
		}

		if (!tasksInited.get()) {
			LOG.info("Cannot create task split for " + profile.getJobID());
			return null;
		}

		int target = -1;
		int speculativeTarget = -1;

		if ((!conf.isIterative())
				|| this.loopMapCacheSwitch.isCacheWritten(conf,
						conf.getCurrentIteration(), conf.getCurrentStep())
				|| conf.getMapperInputCacheOption() == false) {
			target = findNewMapTask(tts, clusterSize, numUniqueHosts,
					this.anyCacheLevel, status.mapProgress());
			if (target == -1) {
				return null;
			}

			if (conf.getMapperInputCacheOption() == true) {
				List<Integer> mapList = taskTrackerToMapTask.get(tts.getHost());
				if (mapList == null) {
					mapList = new ArrayList<Integer>();
					taskTrackerToMapTask.put(tts.getHost(), mapList);
				}
				mapList.add(target);
			}
		} else {
			speculativeTarget = findNewMapTask(tts, clusterSize,
					numUniqueHosts, anyCacheLevel, status.mapProgress());
			List<Integer> mapList = taskTrackerToMapTask.get(tts.getHost());

			boolean scheduled = false;
			for (int i = 0; i < mapList.size(); i++) {
				target = mapList.get(i);
				if (mapScheduled[target] == false) {
					mapScheduled[target] = true;
					scheduled = true;
					break;
				}
			}
			if (!scheduled)
				target = -1;
		}

		if (conf.isIterative()
				&& speculativeTarget >= 0
				&& target < 0
				&& this.isSpeculativeMap[speculativeTarget] == true
				&& mapScheduled[speculativeTarget] == true
				&& !this.loopMapCacheSwitch.isCacheWritten(conf,
						conf.getCurrentIteration(), conf.getCurrentStep())) {
			target = speculativeTarget;
			List<Integer> mapList = taskTrackerToMapTask.get(tts.getHost());
			if (mapList == null) {
				mapList = new ArrayList<Integer>();
				taskTrackerToMapTask.put(tts.getHost(), mapList);
			}
			System.out.println("speculative map executation scheduled "
					+ target + " " + tts.getHost());
			mapList.add(target);
			/**
			 * reset speculative bits
			 */
			this.isSpeculativeMap[target] = false;
		}

		if (target < 0)
			return null;

		Task result = maps[target].getTaskToRun(tts.getTrackerName());

		// set current iteration and step
		result.setCurrentIteration(conf.getCurrentIteration());
		result.setCurrentStep(conf.getCurrentStep());

		if (result != null) {
			addRunningTaskToTIP(maps[target], result.getTaskID(), tts, true);
		}
		result.setConf(conf);
		return result;
	}

	/*
	 * Return task cleanup attempt if any, to run on a given tracker
	 */
	public Task obtainTaskCleanupTask(TaskTrackerStatus tts, boolean isMapSlot)
			throws IOException {
		if (!tasksInited.get()) {
			return null;
		}
		synchronized (this) {
			if (this.status.getRunState() != JobStatus.RUNNING || jobFailed
					|| jobKilled) {
				return null;
			}
			String taskTracker = tts.getTrackerName();
			if (!shouldRunOnTaskTracker(taskTracker)) {
				return null;
			}
			TaskAttemptID taskid = null;
			TaskInProgress tip = null;
			if (isMapSlot) {
				if (!mapCleanupTasks.isEmpty()) {
					taskid = mapCleanupTasks.remove(0);
					tip = maps[taskid.getTaskID().getId()];
				}
			} else {
				if (!reduceCleanupTasks.isEmpty()) {
					taskid = reduceCleanupTasks.remove(0);
					tip = reduces[taskid.getTaskID().getId()];
				}
			}
			if (tip != null && taskid != null && taskTracker != null) {
				return tip.addRunningTask(taskid, taskTracker, true);
			}
			return null;
		}
	}

	public synchronized Task obtainNewLocalMapTask(TaskTrackerStatus tts,
			int clusterSize, int numUniqueHosts) throws IOException {
		if (!tasksInited.get()) {
			LOG.info("Cannot create task split for " + profile.getJobID());
			return null;
		}

		int target = -1;
		int speculativeTarget = -1;

		if ((!conf.isIterative())
				|| this.loopMapCacheSwitch.isCacheWritten(conf,
						conf.getCurrentIteration(), conf.getCurrentStep())
				|| conf.getMapperInputCacheOption() == false) {
			target = findNewMapTask(tts, clusterSize, numUniqueHosts, maxLevel,
					status.mapProgress());
			if (target == -1) {
				return null;
			}

			/**
			 * store tasktracker-><map list> mapping
			 */
			if (conf.getMapperInputCacheOption() == true) {
				List<Integer> mapList = taskTrackerToMapTask.get(tts.getHost());
				if (mapList == null) {
					mapList = new ArrayList<Integer>();
					taskTrackerToMapTask.put(tts.getHost(), mapList);
				}
				mapList.add(target);
			}
		} else {
			speculativeTarget = findNewMapTask(tts, clusterSize,
					numUniqueHosts, maxLevel, status.mapProgress());
			List<Integer> mapList = taskTrackerToMapTask.get(tts.getHost());

			if (mapList == null)
				return null;

			boolean scheduled = false;
			for (int i = 0; i < mapList.size(); i++) {
				target = mapList.get(i);
				if (mapScheduled[target] == false) {
					mapScheduled[target] = true;
					scheduled = true;
					break;
				}
			}
			if (!scheduled)
				target = -1;
		}

		if (conf.isIterative()
				&& speculativeTarget >= 0
				&& target < 0
				&& this.isSpeculativeMap[speculativeTarget]
				&& mapScheduled[speculativeTarget] == true
				&& !this.loopMapCacheSwitch.isCacheWritten(conf,
						conf.getCurrentIteration(), conf.getCurrentStep())) {
			target = speculativeTarget;
			List<Integer> mapList = taskTrackerToMapTask.get(tts.getHost());
			if (mapList == null) {
				mapList = new ArrayList<Integer>();
				taskTrackerToMapTask.put(tts.getHost(), mapList);
			}
			System.out.println("speculative map executation scheduled "
					+ target + " " + tts.getHost());
			mapList.add(target);
			/**
			 * reset speculative bits
			 */
			this.isSpeculativeMap[target] = false;
		}

		if (target < 0)
			return null;

		Task result = maps[target].getTaskToRun(tts.getTrackerName());

		// set current step and iteration
		result.setCurrentIteration(conf.getCurrentIteration());
		result.setCurrentStep(conf.getCurrentStep());

		if (result != null) {
			addRunningTaskToTIP(maps[target], result.getTaskID(), tts, true);
		}
		result.setConf(conf);
		return result;
	}

	public synchronized Task obtainNewNonLocalMapTask(TaskTrackerStatus tts,
			int clusterSize, int numUniqueHosts) throws IOException {

		if (!tasksInited.get()) {
			LOG.info("Cannot create task split for " + profile.getJobID());
			return null;
		}

		int target = -1;
		int speculativeTarget = -1;

		if ((!conf.isIterative())
				|| this.loopMapCacheSwitch.isCacheWritten(conf,
						conf.getCurrentIteration(), conf.getCurrentStep())
				|| conf.getMapperInputCacheOption() == false) {
			target = findNewMapTask(tts, clusterSize, numUniqueHosts,
					NON_LOCAL_CACHE_LEVEL, status.mapProgress());
			if (target == -1) {
				return null;
			}
			if (conf.getMapperInputCacheOption() == true) {
				List<Integer> mapList = taskTrackerToMapTask.get(tts.getHost());
				if (mapList == null) {
					mapList = new ArrayList<Integer>();
					taskTrackerToMapTask.put(tts.getHost(), mapList);
				}
				mapList.add(target);
			}
		} else {
			speculativeTarget = findNewMapTask(tts, clusterSize,
					numUniqueHosts, NON_LOCAL_CACHE_LEVEL, status.mapProgress());

			List<Integer> mapList = taskTrackerToMapTask.get(tts.getHost());

			if (mapList == null)
				return null;

			boolean scheduled = false;

			for (int i = 0; i < mapList.size(); i++) {
				target = mapList.get(i);
				if (mapScheduled[target] == false) {
					mapScheduled[target] = true;
					scheduled = true;
					break;
				}
			}

			if (!scheduled)
				target = -1;
		}

		if (conf.isIterative()
				&& speculativeTarget >= 0
				&& target < 0
				&& this.isSpeculativeMap[speculativeTarget]
				&& mapScheduled[speculativeTarget] == true
				&& !this.loopMapCacheSwitch.isCacheWritten(conf,
						conf.getCurrentIteration(), conf.getCurrentStep())) {
			target = speculativeTarget;
			List<Integer> mapList = taskTrackerToMapTask.get(tts.getHost());
			if (mapList == null) {
				mapList = new ArrayList<Integer>();
				taskTrackerToMapTask.put(tts.getHost(), mapList);
			}
			System.out.println("speculative map executation scheduled "
					+ target + " " + tts.getHost());
			mapList.add(target);
			/**
			 * reset speculative bits
			 */
			this.isSpeculativeMap[target] = false;
		}

		if (target < 0)
			return null;

		Task result = maps[target].getTaskToRun(tts.getTrackerName());
		// set current step and iteration
		result.setCurrentIteration(conf.getCurrentIteration());
		result.setCurrentStep(conf.getCurrentStep());

		if (result != null) {
			addRunningTaskToTIP(maps[target], result.getTaskID(), tts, true);
		}
		result.setConf(conf);
		return result;
	}

	/**
	 * Return a CleanupTask, if appropriate, to run on the given tasktracker
	 * 
	 */
	public Task obtainJobCleanupTask(TaskTrackerStatus tts, int clusterSize,
			int numUniqueHosts, boolean isMapSlot) throws IOException {
		if (!tasksInited.get()) {
			return null;
		}

		synchronized (this) {
			if (!canLaunchJobCleanupTask()) {
				return null;
			}

			String taskTracker = tts.getTrackerName();
			// Update the last-known clusterSize
			this.clusterSize = clusterSize;
			if (!shouldRunOnTaskTracker(taskTracker)) {
				return null;
			}

			List<TaskInProgress> cleanupTaskList = new ArrayList<TaskInProgress>();
			if (isMapSlot) {
				cleanupTaskList.add(cleanup[0]);
			} else {
				cleanupTaskList.add(cleanup[1]);
			}
			TaskInProgress tip = findTaskFromList(cleanupTaskList, tts,
					numUniqueHosts, false);
			if (tip == null) {
				return null;
			}

			// Now launch the cleanupTask
			Task result = tip.getTaskToRun(tts.getTrackerName());
			if (result != null) {
				addRunningTaskToTIP(tip, result.getTaskID(), tts, true);
			}
			return result;
		}

	}

	/**
	 * Check whether cleanup task can be launched for the job.
	 * 
	 * Cleanup task can be launched if it is not already launched or job is
	 * Killed or all maps and reduces are complete
	 * 
	 * @return true/false
	 */
	private synchronized boolean canLaunchJobCleanupTask() {
		// check if the job is running
		if (status.getRunState() != JobStatus.RUNNING
				&& status.getRunState() != JobStatus.PREP) {
			return false;
		}
		// check if cleanup task has been launched already or if setup isn't
		// launched already. The later check is useful when number of maps is
		// zero.
		if (launchedCleanup || !isSetupFinished()) {
			return false;
		}
		// check if job has failed or killed
		if (jobKilled || jobFailed) {
			return true;
		}
		// Check if all maps and reducers have finished.
		boolean launchCleanupTask = ((finishedMapTasks + failedMapTIPs) == (numMapTasks));
		if (launchCleanupTask) {
			launchCleanupTask = ((finishedReduceTasks + failedReduceTIPs) == numReduceTasks);
		}
		// Yingyi: augment the condition by: all iterations are done
		// for test purpose
		// conf.setIterative(true);
		if (!conf.isIterative()) {
			// if it is not iterative processing
			return launchCleanupTask;
		} else
			return canExist;
	}

	/**
	 * Return a SetupTask, if appropriate, to run on the given tasktracker
	 * 
	 */
	public Task obtainJobSetupTask(TaskTrackerStatus tts, int clusterSize,
			int numUniqueHosts, boolean isMapSlot) throws IOException {
		if (!tasksInited.get()) {
			return null;
		}

		synchronized (this) {
			if (!canLaunchSetupTask()) {
				return null;
			}
			String taskTracker = tts.getTrackerName();
			// Update the last-known clusterSize
			this.clusterSize = clusterSize;
			if (!shouldRunOnTaskTracker(taskTracker)) {
				return null;
			}

			List<TaskInProgress> setupTaskList = new ArrayList<TaskInProgress>();
			if (isMapSlot) {
				setupTaskList.add(setup[0]);
			} else {
				setupTaskList.add(setup[1]);
			}
			TaskInProgress tip = findTaskFromList(setupTaskList, tts,
					numUniqueHosts, false);
			if (tip == null) {
				return null;
			}

			// Now launch the setupTask
			Task result = tip.getTaskToRun(tts.getTrackerName());
			if (result != null) {
				addRunningTaskToTIP(tip, result.getTaskID(), tts, true);
			}
			return result;
		}
	}

	public synchronized boolean scheduleReduces() {
		return finishedMapTasks >= completedMapsForReduceSlowstart;
	}

	/**
	 * Check whether setup task can be launched for the job.
	 * 
	 * Setup task can be launched after the tasks are inited and Job is in PREP
	 * state and if it is not already launched or job is not Killed/Failed
	 * 
	 * @return true/false
	 */
	private synchronized boolean canLaunchSetupTask() {
		return (tasksInited.get() && status.getRunState() == JobStatus.PREP
				&& !launchedSetup && !jobKilled && !jobFailed);
	}

	/**
	 * Return a ReduceTask, if appropriate, to run on the given tasktracker. We
	 * don't have cache-sensitivity for reduce tasks, as they work on temporary
	 * MapRed files.
	 */
	public synchronized Task obtainNewReduceTask(TaskTrackerStatus tts,
			int clusterSize, int numUniqueHosts) throws IOException {
		if (status.getRunState() != JobStatus.RUNNING) {
			LOG.info("Cannot create task split for " + profile.getJobID());
			return null;
		}

		// Ensure we have sufficient map outputs ready to shuffle before
		// scheduling reduces
		if (!scheduleReduces()) {
			return null;
		}

		int target = -1;
		int speculativeTarget = -1;
		int pass = conf.getCurrentIteration() * conf.getNumberOfLoopBodySteps()
				+ conf.getCurrentStep();

		if ((!conf.isIterative())
				|| this.loopReduceCacheSwitch.isCacheWritten(conf,
						conf.getCurrentIteration(), conf.getCurrentStep())
				|| conf.getMapperInputCacheOption() == true) {
			target = findNewReduceTask(tts, clusterSize, numUniqueHosts,
					status.reduceProgress());
			if (target == -1) {
				System.out.println("return null reducer in job-in-progress");
				return null;
			}
		} else {
			speculativeTarget = findNewReduceTask(tts, clusterSize,
					numUniqueHosts, status.reduceProgress());

			if (conf.isIterative()
					&& !this.loopReduceCacheSwitch.isCacheRead(conf,
							conf.getCurrentIteration(), conf.getCurrentStep())
					&& !this.loopReduceCacheSwitch.isCacheWritten(conf,
							conf.getCurrentIteration(), conf.getCurrentStep())
					&& !this.loopReduceOutputCacheSwitch.isCacheRead(conf,
							conf.getCurrentIteration(), conf.getCurrentStep())
					&& !this.loopReduceOutputCacheSwitch.isCacheWritten(conf,
							conf.getCurrentIteration(), conf.getCurrentStep())) {
				target = speculativeTarget;
			} else {
				List<Integer> reduceList = taskTrackerToReduceTask.get(tts
						.getHost());
				if (reduceList == null)
					target = -1;
				else {
					boolean scheduled = false;
					int pos = -1;
					for (int i = 0; i < reduceList.size(); i++) {
						target = reduceList.get(i);
						if (reduceScheduled[target] == false) {
							reduceScheduled[target] = true;
							scheduled = true;
							pos = i;
							break;
						}
					}
					if (scheduled)
						reduceList.remove(pos);
					else
						target = -1;
				}
			}
		}

		String recoverFrom = "xxx";
		boolean nodeFailure = false;
		if (conf.isIterative() && speculativeTarget >= 0 && target < 0
				&& isSpeculativeReduce[speculativeTarget]
				&& reduceScheduled[speculativeTarget] == true) {

			target = speculativeTarget;
			System.out.println("speculative reduce executation scheduled "
					+ target + " " + tts.getHost());

			String host = this.reducePartitionToTts.get(target);
			if (jobtracker.getFaultyTrackers().isBlacklisted(host)) {
				/**
				 * in the case of node failure
				 */
				recoverFrom = host;
				nodeFailure = true;
			}

			/**
			 * reset speculative bits
			 */
			this.isSpeculativeReduce[target] = false;
		}

		if (target < 0)
			return null;

		Task result = reduces[target].getTaskToRun(tts.getTrackerName());
		result.setRecoverFromTaskTracker(recoverFrom);
		result.setNodeFailure(nodeFailure);

		// Yingyi: set current iteration and step
		if (conf.isIterative()) {
			result.setCurrentIteration(currentRounds / specifiedSteps);
			result.setCurrentStep(conf.getCurrentStep());
		}

		if (result != null) {
			addRunningTaskToTIP(reduces[target], result.getTaskID(), tts, true);
		}
		result.setConf(conf);
		return result;
	}

	// returns the (cache)level at which the nodes matches
	private int getMatchingLevelForNodes(Node n1, Node n2) {
		int count = 0;
		do {
			if (n1.equals(n2)) {
				return count;
			}
			++count;
			n1 = n1.getParent();
			n2 = n2.getParent();
		} while (n1 != null);
		return this.maxLevel;
	}

	/**
	 * Populate the data structures as a task is scheduled.
	 * 
	 * Assuming {@link JobTracker} is locked on entry.
	 * 
	 * @param tip
	 *            The tip for which the task is added
	 * @param id
	 *            The attempt-id for the task
	 * @param tts
	 *            task-tracker status
	 * @param isScheduled
	 *            Whether this task is scheduled from the JT or has joined back
	 *            upon restart
	 */
	synchronized void addRunningTaskToTIP(TaskInProgress tip, TaskAttemptID id,
			TaskTrackerStatus tts, boolean isScheduled) {
		// Make an entry in the tip if the attempt is not scheduled i.e
		// externally
		// added
		if (!isScheduled) {
			tip.addRunningTask(id, tts.getTrackerName());
		}
		final JobTrackerInstrumentation metrics = jobtracker
				.getInstrumentation();

		// keeping the earlier ordering intact
		String name;
		String splits = "";
		Enum counter = null;
		if (tip.isJobSetupTask()) {
			launchedSetup = true;
			name = Values.SETUP.name();
		} else if (tip.isJobCleanupTask()) {
			launchedCleanup = true;
			name = Values.CLEANUP.name();
		} else if (tip.isMapTask()) {
			++runningMapTasks;
			name = Values.MAP.name();
			counter = Counter.TOTAL_LAUNCHED_MAPS;
			splits = tip.getSplitNodes();
			if (tip.getActiveTasks().size() > 1)
				speculativeMapTasks++;
			metrics.launchMap(id);
		} else {
			++runningReduceTasks;
			name = Values.REDUCE.name();
			counter = Counter.TOTAL_LAUNCHED_REDUCES;
			if (tip.getActiveTasks().size() > 1)
				speculativeReduceTasks++;
			metrics.launchReduce(id);
		}
		// Note that the logs are for the scheduled tasks only. Tasks that join
		// on
		// restart has already their logs in place.
		if (tip.isFirstAttempt(id)) {
			JobHistory.Task.logStarted(tip.getTIPId(), name,
					tip.getExecStartTime(), splits);
		}
		if (!tip.isJobSetupTask() && !tip.isJobCleanupTask()) {
			jobCounters.incrCounter(counter, 1);
		}

		// TODO The only problem with these counters would be on restart.
		// The jobtracker updates the counter only when the task that is
		// scheduled
		// if from a non-running tip and is local (data, rack ...). But upon
		// restart
		// as the reports come from the task tracker, there is no good way to
		// infer
		// when exactly to increment the locality counters. The only solution is
		// to
		// increment the counters for all the tasks irrespective of
		// - whether the tip is running or not
		// - whether its a speculative task or not
		//
		// So to simplify, increment the data locality counter whenever there is
		// data locality.
		if (tip.isMapTask() && !tip.isJobSetupTask() && !tip.isJobCleanupTask()) {
			// increment the data locality counter for maps
			Node tracker = jobtracker.getNode(tts.getHost());
			int level = this.maxLevel;
			// find the right level across split locations
			for (String local : maps[tip.getIdWithinJob()].getSplitLocations()) {
				Node datanode = jobtracker.getNode(local);
				int newLevel = this.maxLevel;
				if (tracker != null && datanode != null) {
					newLevel = getMatchingLevelForNodes(tracker, datanode);
				}
				if (newLevel < level) {
					level = newLevel;
					// an optimization
					if (level == 0) {
						break;
					}
				}
			}
			switch (level) {
			case 0:
				LOG.info("Choosing data-local task " + tip.getTIPId());
				jobCounters.incrCounter(Counter.DATA_LOCAL_MAPS, 1);
				break;
			case 1:
				LOG.info("Choosing rack-local task " + tip.getTIPId());
				jobCounters.incrCounter(Counter.RACK_LOCAL_MAPS, 1);
				break;
			default:
				// check if there is any locality
				if (level != this.maxLevel) {
					LOG.info("Choosing cached task at level " + level
							+ tip.getTIPId());
					jobCounters.incrCounter(Counter.OTHER_LOCAL_MAPS, 1);
				}
				break;
			}
		}
	}

	static String convertTrackerNameToHostName(String trackerName) {
		// Ugly!
		// Convert the trackerName to it's host name
		int indexOfColon = trackerName.indexOf(":");
		String trackerHostName = (indexOfColon == -1) ? trackerName
				: trackerName.substring(0, indexOfColon);
		return trackerHostName.substring("tracker_".length());
	}

	/**
	 * Note that a task has failed on a given tracker and add the tracker to the
	 * blacklist iff too many trackers in the cluster i.e. (clusterSize *
	 * CLUSTER_BLACKLIST_PERCENT) haven't turned 'flaky' already.
	 * 
	 * @param trackerName
	 *            task-tracker on which a task failed
	 */
	void addTrackerTaskFailure(String trackerName) {

		if (flakyTaskTrackers < (clusterSize * CLUSTER_BLACKLIST_PERCENT)) {
			String trackerHostName = convertTrackerNameToHostName(trackerName);

			Integer trackerFailures = trackerToFailuresMap.get(trackerHostName);
			if (trackerFailures == null) {
				trackerFailures = 0;
			}
			trackerToFailuresMap.put(trackerHostName, ++trackerFailures);

			// Check if this tasktracker has turned 'flaky'
			if (trackerFailures.intValue() == conf
					.getMaxTaskFailuresPerTracker()) {
				++flakyTaskTrackers;
				LOG.info("TaskTracker at '" + trackerHostName
						+ "' turned 'flaky'");
			}
		}
	}

	private int getTrackerTaskFailures(String trackerName) {
		String trackerHostName = convertTrackerNameToHostName(trackerName);
		Integer failedTasks = trackerToFailuresMap.get(trackerHostName);
		return (failedTasks != null) ? failedTasks.intValue() : 0;
	}

	/**
	 * Get the black listed trackers for the job
	 * 
	 * @return List of blacklisted tracker names
	 */
	List<String> getBlackListedTrackers() {
		List<String> blackListedTrackers = new ArrayList<String>();
		for (Map.Entry<String, Integer> e : trackerToFailuresMap.entrySet()) {
			if (e.getValue().intValue() >= conf.getMaxTaskFailuresPerTracker()) {
				blackListedTrackers.add(e.getKey());
			}
		}
		return blackListedTrackers;
	}

	/**
	 * Get the no. of 'flaky' tasktrackers for a given job.
	 * 
	 * @return the no. of 'flaky' tasktrackers for a given job.
	 */
	int getNoOfBlackListedTrackers() {
		return flakyTaskTrackers;
	}

	/**
	 * Get the information on tasktrackers and no. of errors which occurred on
	 * them for a given job.
	 * 
	 * @return the map of tasktrackers and no. of errors which occurred on them
	 *         for a given job.
	 */
	synchronized Map<String, Integer> getTaskTrackerErrors() {
		// Clone the 'trackerToFailuresMap' and return the copy
		Map<String, Integer> trackerErrors = new TreeMap<String, Integer>(
				trackerToFailuresMap);
		return trackerErrors;
	}

	/**
	 * Remove a map TIP from the lists for running maps. Called when a map
	 * fails/completes (note if a map is killed, it won't be present in the list
	 * since it was completed earlier)
	 * 
	 * @param tip
	 *            the tip that needs to be retired
	 */
	private synchronized void retireMap(TaskInProgress tip) {
		if (runningMapCache == null) {
			LOG.warn("Running cache for maps missing!! "
					+ "Job details are missing.");
			return;
		}

		String[] splitLocations = tip.getSplitLocations();

		// Remove the TIP from the list for running non-local maps
		if (splitLocations.length == 0) {
			nonLocalRunningMaps.remove(tip);
			return;
		}

		// Remove from the running map caches
		for (String host : splitLocations) {
			Node node = jobtracker.getNode(host);

			for (int j = 0; j < maxLevel; ++j) {
				Set<TaskInProgress> hostMaps = runningMapCache.get(node);
				if (hostMaps != null) {
					hostMaps.remove(tip);
					if (hostMaps.size() == 0) {
						runningMapCache.remove(node);
					}
				}
				node = node.getParent();
			}
		}
	}

	/**
	 * Remove a reduce TIP from the list for running-reduces Called when a
	 * reduce fails/completes
	 * 
	 * @param tip
	 *            the tip that needs to be retired
	 */
	private synchronized void retireReduce(TaskInProgress tip) {
		if (runningReduces == null) {
			LOG.warn("Running list for reducers missing!! "
					+ "Job details are missing.");
			return;
		}
		runningReduces.remove(tip);
	}

	/**
	 * Adds a map tip to the list of running maps.
	 * 
	 * @param tip
	 *            the tip that needs to be scheduled as running
	 */
	private synchronized void scheduleMap(TaskInProgress tip) {

		if (runningMapCache == null) {
			LOG.warn("Running cache for maps is missing!! "
					+ "Job details are missing.");
			return;
		}
		String[] splitLocations = tip.getSplitLocations();

		// Add the TIP to the list of non-local running TIPs
		if (splitLocations.length == 0) {
			nonLocalRunningMaps.add(tip);
			return;
		}

		for (String host : splitLocations) {
			Node node = jobtracker.getNode(host);

			for (int j = 0; j < maxLevel; ++j) {
				Set<TaskInProgress> hostMaps = runningMapCache.get(node);
				if (hostMaps == null) {
					// create a cache if needed
					hostMaps = new LinkedHashSet<TaskInProgress>();
					runningMapCache.put(node, hostMaps);
				}
				hostMaps.add(tip);
				node = node.getParent();
			}
		}
	}

	/**
	 * Adds a reduce tip to the list of running reduces
	 * 
	 * @param tip
	 *            the tip that needs to be scheduled as running
	 */
	private synchronized void scheduleReduce(TaskInProgress tip) {
		if (runningReduces == null) {
			LOG.warn("Running cache for reducers missing!! "
					+ "Job details are missing.");
			return;
		}
		runningReduces.add(tip);
	}

	boolean failTrackerMap = false;

	/**
	 * Adds the failed TIP in the front of the list for non-running maps
	 * 
	 * @param tip
	 *            the tip that needs to be failed
	 */
	private synchronized void failMap(TaskInProgress tip, TaskTrackerStatus tts) {
		if (nonRunningMapCache == null) {
			LOG.warn("Non-running cache for maps missing!! "
					+ "Job details are missing.");
			return;
		}

		if (!failTrackerMap) {
			int target = tip.getIdWithinJob();
			List<Integer> mapList = this.taskTrackerToMapTask
					.get(tts.getHost());
			if (mapList != null)
				mapList.remove(new Integer(target));
			this.isSpeculativeMap[target] = true;
		} else {
			List<Integer> mapList = this.taskTrackerToMapTask
					.get(tts.getHost());
			if (mapList != null) {
				int i = mapList.size() - 1;
				for (; i >= 0; i--) {
					int target = mapList.remove(i);
					isSpeculativeMap[target] = true;
				}
			}
		}

		// 1. Its added everywhere since other nodes (having this split local)
		// might have removed this tip from their local cache
		// 2. Give high priority to failed tip - fail early

		String[] splitLocations = tip.getSplitLocations();

		// Add the TIP in the front of the list for non-local non-running maps
		if (splitLocations.length == 0) {
			nonLocalMaps.add(0, tip);
			return;
		}

		for (String host : splitLocations) {
			Node node = jobtracker.getNode(host);

			for (int j = 0; j < maxLevel; ++j) {
				List<TaskInProgress> hostMaps = nonRunningMapCache.get(node);
				if (hostMaps == null) {
					hostMaps = new LinkedList<TaskInProgress>();
					nonRunningMapCache.put(node, hostMaps);
				}
				hostMaps.add(0, tip);
				node = node.getParent();
			}
		}
	}

	/**
	 * Adds a failed TIP in the front of the list for non-running reduces
	 * 
	 * @param tip
	 *            the tip that needs to be failed
	 */
	private synchronized void failReduce(TaskInProgress tip,
			TaskTrackerStatus tts) {
		if (nonRunningReduces == null) {
			LOG.warn("Failed cache for reducers missing!! "
					+ "Job details are missing.");
			return;
		}

		// int target = tip.getIdWithinJob();
		// if (tts != null) {
		// List<Integer> reduceList = this.taskTrackerToReduceTask.get(tts
		// .getHost());
		// if (reduceList != null)
		// reduceList.remove(new Integer(target));
		// this.isSpeculativeReduce[target] = true;
		// }
		nonRunningReduces.add(0, tip);
	}

	/**
	 * Find a non-running task in the passed list of TIPs
	 * 
	 * @param tips
	 *            a collection of TIPs
	 * @param ttStatus
	 *            the status of tracker that has requested a task to run
	 * @param numUniqueHosts
	 *            number of unique hosts that run trask trackers
	 * @param removeFailedTip
	 *            whether to remove the failed tips
	 */
	private synchronized TaskInProgress findTaskFromList(
			Collection<TaskInProgress> tips, TaskTrackerStatus ttStatus,
			int numUniqueHosts, boolean removeFailedTip) {
		Iterator<TaskInProgress> iter = tips.iterator();
		while (iter.hasNext()) {
			TaskInProgress tip = iter.next();

			// Select a tip if
			// 1. runnable : still needs to be run and is not completed
			// 2. ~running : no other node is running it
			// 3. earlier attempt failed : has not failed on this host
			// and has failed on all the other hosts
			// A TIP is removed from the list if
			// (1) this tip is scheduled
			// (2) if the passed list is a level 0 (host) cache
			// (3) when the TIP is non-schedulable (running, killed, complete)
			if (tip.isRunnable() && !tip.isRunning()) {
				// check if the tip has failed on this host
				if (!tip.hasFailedOnMachine(ttStatus.getHost())
						|| tip.getNumberOfFailedMachines() >= numUniqueHosts) {
					// check if the tip has failed on all the nodes
					iter.remove();
					return tip;
				} else if (removeFailedTip) {
					// the case where we want to remove a failed tip from the
					// host cache
					// point#3 in the TIP removal logic above
					iter.remove();
				}
			} else {
				// see point#3 in the comment above for TIP removal logic
				iter.remove();
			}
		}
		return null;
	}

	/**
	 * Find a speculative task
	 * 
	 * @param list
	 *            a list of tips
	 * @param taskTracker
	 *            the tracker that has requested a tip
	 * @param avgProgress
	 *            the average progress for speculation
	 * @param currentTime
	 *            current time in milliseconds
	 * @param shouldRemove
	 *            whether to remove the tips
	 * @return a tip that can be speculated on the tracker
	 */
	private synchronized TaskInProgress findSpeculativeTask(
			Collection<TaskInProgress> list, TaskTrackerStatus ttStatus,
			double avgProgress, long currentTime, boolean shouldRemove) {

		Iterator<TaskInProgress> iter = list.iterator();

		while (iter.hasNext()) {
			TaskInProgress tip = iter.next();
			// should never be true! (since we delete completed/failed tasks)
			if (!tip.isRunning()) {
				iter.remove();
				continue;
			}

			if (!tip.hasRunOnMachine(ttStatus.getHost(),
					ttStatus.getTrackerName())) {
				if (tip.hasSpeculativeTask(currentTime, avgProgress)) {
					// In case of shared list we don't remove it. Since the TIP
					// failed
					// on this tracker can be scheduled on some other tracker.
					if (shouldRemove) {
						iter.remove(); // this tracker is never going to run it
						// again
					}
					int target = tip.getIdWithinJob();
					{
						if (tip.isMapTask())
							this.isSpeculativeMap[target] = true;
						else
							this.isSpeculativeReduce[target] = true;
					}
					return tip;
				}
			} else {
				// Check if this tip can be removed from the list.
				// If the list is shared then we should not remove.
				if (shouldRemove) {
					// This tracker will never speculate this tip
					iter.remove();
				}
			}
		}
		return null;
	}

	/**
	 * Find new map task
	 * 
	 * @param tts
	 *            The task tracker that is asking for a task
	 * @param clusterSize
	 *            The number of task trackers in the cluster
	 * @param numUniqueHosts
	 *            The number of hosts that run task trackers
	 * @param avgProgress
	 *            The average progress of this kind of task in this job
	 * @param maxCacheLevel
	 *            The maximum topology level until which to schedule maps. A
	 *            value of {@link #anyCacheLevel} implies any available task
	 *            (node-local, rack-local, off-switch and speculative tasks). A
	 *            value of {@link #NON_LOCAL_CACHE_LEVEL} implies only
	 *            off-switch/speculative tasks should be scheduled.
	 * @return the index in tasks of the selected task (or -1 for no task)
	 */
	private synchronized int findNewMapTask(final TaskTrackerStatus tts,
			final int clusterSize, final int numUniqueHosts,
			final int maxCacheLevel, final double avgProgress) {
		// System.out.println(numMapTasks + " mappers are to be scheduled");
		if (numMapTasks == 0) {
			LOG.info("No maps to schedule for " + profile.getJobID());
			return -1;
		}

		String taskTracker = tts.getTrackerName();
		TaskInProgress tip = null;

		//
		// Update the last-known clusterSize
		//
		this.clusterSize = clusterSize;

		if (!shouldRunOnTaskTracker(taskTracker)) {
			return -1;
		}

		// Check to ensure this TaskTracker has enough resources to
		// run tasks from this job
		long outSize = resourceEstimator.getEstimatedMapOutputSize();
		long availSpace = tts.getResourceStatus().getAvailableSpace();
		if (availSpace < outSize) {
			LOG.warn("No room for map task. Node " + tts.getHost() + " has "
					+ availSpace + " bytes free; but we expect map to take "
					+ outSize);

			return -1; // see if a different TIP might work better.
		}

		// For scheduling a map task, we have two caches and a list (optional)
		// I) one for non-running task
		// II) one for running task (this is for handling speculation)
		// III) a list of TIPs that have empty locations (e.g., dummy splits),
		// the list is empty if all TIPs have associated locations

		// First a look up is done on the non-running cache and on a miss, a
		// look
		// up is done on the running cache. The order for lookup within the
		// cache:
		// 1. from local node to root [bottom up]
		// 2. breadth wise for all the parent nodes at max level

		// We fall to linear scan of the list (III above) if we have misses in
		// the
		// above caches

		Node node = jobtracker.getNode(tts.getHost());

		//
		// I) Non-running TIP :
		//

		// 1. check from local node to the root [bottom up cache lookup]
		// i.e if the cache is available and the host has been resolved
		// (node!=null)
		if (node != null) {
			Node key = node;
			int level = 0;
			// maxCacheLevel might be greater than this.maxLevel if
			// findNewMapTask is
			// called to schedule any task (local, rack-local, off-switch or
			// speculative)
			// tasks or it might be NON_LOCAL_CACHE_LEVEL (i.e. -1) if
			// findNewMapTask is
			// (i.e. -1) if findNewMapTask is to only schedule
			// off-switch/speculative
			// tasks
			int maxLevelToSchedule = Math.min(maxCacheLevel, maxLevel);
			for (level = 0; level < maxLevelToSchedule; ++level) {
				List<TaskInProgress> cacheForLevel = nonRunningMapCache
						.get(key);
				if (cacheForLevel != null) {
					tip = findTaskFromList(cacheForLevel, tts, numUniqueHosts,
							level == 0);
					if (tip != null) {
						// Add to running cache
						scheduleMap(tip);

						// remove the cache if its empty
						if (cacheForLevel.size() == 0) {
							nonRunningMapCache.remove(key);
						}

						return tip.getIdWithinJob();
					}
				}
				key = key.getParent();
			}

			// Check if we need to only schedule a local task
			// (node-local/rack-local)
			if (level == maxCacheLevel) {
				return -1;
			}
		}

		// 2. Search breadth-wise across parents at max level for non-running
		// TIP if
		// - cache exists and there is a cache miss
		// - node information for the tracker is missing (tracker's topology
		// info not obtained yet)

		// collection of node at max level in the cache structure
		Collection<Node> nodesAtMaxLevel = jobtracker.getNodesAtMaxLevel();

		// get the node parent at max level
		Node nodeParentAtMaxLevel = (node == null) ? null : JobTracker
				.getParentNode(node, maxLevel - 1);

		for (Node parent : nodesAtMaxLevel) {

			// skip the parent that has already been scanned
			if (parent == nodeParentAtMaxLevel) {
				continue;
			}

			List<TaskInProgress> cache = nonRunningMapCache.get(parent);
			if (cache != null) {
				tip = findTaskFromList(cache, tts, numUniqueHosts, false);
				if (tip != null) {
					// Add to the running cache
					scheduleMap(tip);

					// remove the cache if empty
					if (cache.size() == 0) {
						nonRunningMapCache.remove(parent);
					}
					LOG.info("Choosing a non-local task " + tip.getTIPId());
					return tip.getIdWithinJob();
				}
			}
		}

		// 3. Search non-local tips for a new task
		tip = findTaskFromList(nonLocalMaps, tts, numUniqueHosts, false);
		if (tip != null) {
			// Add to the running list
			scheduleMap(tip);

			LOG.info("Choosing a non-local task " + tip.getTIPId());
			return tip.getIdWithinJob();
		}

		//
		// II) Running TIP :
		//

		if (hasSpeculativeMaps) {
			long currentTime = System.currentTimeMillis();

			// 1. Check bottom up for speculative tasks from the running cache
			if (node != null) {
				Node key = node;
				for (int level = 0; level < maxLevel; ++level) {
					Set<TaskInProgress> cacheForLevel = runningMapCache
							.get(key);
					if (cacheForLevel != null) {
						tip = findSpeculativeTask(cacheForLevel, tts,
								avgProgress, currentTime, level == 0);
						if (tip != null) {
							if (cacheForLevel.size() == 0) {
								runningMapCache.remove(key);
							}
							return tip.getIdWithinJob();
						}
					}
					key = key.getParent();
				}
			}

			// 2. Check breadth-wise for speculative tasks

			for (Node parent : nodesAtMaxLevel) {
				// ignore the parent which is already scanned
				if (parent == nodeParentAtMaxLevel) {
					continue;
				}

				Set<TaskInProgress> cache = runningMapCache.get(parent);
				if (cache != null) {
					tip = findSpeculativeTask(cache, tts, avgProgress,
							currentTime, false);
					if (tip != null) {
						// remove empty cache entries
						if (cache.size() == 0) {
							runningMapCache.remove(parent);
						}
						LOG.info("Choosing a non-local task " + tip.getTIPId()
								+ " for speculation");
						return tip.getIdWithinJob();
					}
				}
			}

			// 3. Check non-local tips for speculation
			tip = findSpeculativeTask(nonLocalRunningMaps, tts, avgProgress,
					currentTime, false);
			if (tip != null) {
				LOG.info("Choosing a non-local task " + tip.getTIPId()
						+ " for speculation");
				return tip.getIdWithinJob();
			}
		}

		return -1;
	}

	/**
	 * Find new reduce task
	 * 
	 * @param tts
	 *            The task tracker that is asking for a task
	 * @param clusterSize
	 *            The number of task trackers in the cluster
	 * @param numUniqueHosts
	 *            The number of hosts that run task trackers
	 * @param avgProgress
	 *            The average progress of this kind of task in this job
	 * @return the index in tasks of the selected task (or -1 for no task)
	 */
	private synchronized int findNewReduceTask(TaskTrackerStatus tts,
			int clusterSize, int numUniqueHosts, double avgProgress) {
		if (numReduceTasks == 0) {
			LOG.info("No reduces to schedule for " + profile.getJobID());
			return -1;
		}

		String taskTracker = tts.getTrackerName();
		TaskInProgress tip = null;

		// Update the last-known clusterSize
		this.clusterSize = clusterSize;

		if (!shouldRunOnTaskTracker(taskTracker)) {
			return -1;
		}

		long outSize = resourceEstimator.getEstimatedReduceInputSize();
		long availSpace = tts.getResourceStatus().getAvailableSpace();
		if (availSpace < outSize) {
			LOG.warn("No room for reduce task. Node " + taskTracker + " has "
					+ availSpace
					+ " bytes free; but we expect reduce input to take "
					+ outSize);

			return -1; // see if a different TIP might work better.
		}

		// 1. check for a never-executed reduce tip
		// reducers don't have a cache and so pass -1 to explicitly call that
		// out
		tip = findTaskFromList(nonRunningReduces, tts, numUniqueHosts, false);
		if (tip != null) {
			scheduleReduce(tip);
			return tip.getIdWithinJob();
		}

		// 2. check for a reduce tip to be speculated
		if (hasSpeculativeReduces) {
			tip = findSpeculativeTask(runningReduces, tts, avgProgress,
					System.currentTimeMillis(), false);
			if (tip != null) {
				scheduleReduce(tip);
				return tip.getIdWithinJob();
			}
		}

		return -1;
	}

	private boolean shouldRunOnTaskTracker(String taskTracker) {
		//
		// Check if too many tasks of this job have failed on this
		// tasktracker prior to assigning it a new one.
		//
		int taskTrackerFailedTasks = getTrackerTaskFailures(taskTracker);
		if ((flakyTaskTrackers < (clusterSize * CLUSTER_BLACKLIST_PERCENT))
				&& taskTrackerFailedTasks >= conf
						.getMaxTaskFailuresPerTracker()) {
			if (LOG.isDebugEnabled()) {
				String flakyTracker = convertTrackerNameToHostName(taskTracker);
				LOG.debug("Ignoring the black-listed tasktracker: '"
						+ flakyTracker + "' for assigning a new task");
			}
			return false;
		}
		return true;
	}

	/**
	 * A taskid assigned to this JobInProgress has reported in successfully.
	 */
	public synchronized boolean completedTask(TaskInProgress tip,
			TaskStatus status) {

		// /**
		// * HaLoop: set the binding between partition and tts for reducers
		// */
		// if (conf.isIterative() && conf.getMapperInputCacheOption() == false
		// && !tip.isMapTask() && !tip.isJobCleanupTask()
		// && !tip.isJobSetupTask()) {
		// int target = tip.getIdWithinJob();
		// if (target >= 0) {
		// // get host name from tracker name
		// String[] strs = status.getTaskTracker().split(":");
		// String[] trackers = strs[0].split("_");
		// this.reducePartitionToTts.put(target, trackers[1]);
		// }
		// }

		TaskAttemptID taskid = status.getTaskID();
		int oldNumAttempts = tip.getActiveTasks().size();
		final JobTrackerInstrumentation metrics = jobtracker
				.getInstrumentation();

		// Sanity check: is the TIP already complete?
		// It _is_ safe to not decrement running{Map|Reduce}Tasks and
		// finished{Map|Reduce}Tasks variables here because one and only
		// one task-attempt of a TIP gets to completedTask. This is because
		// the TaskCommitThread in the JobTracker marks other, completed,
		// speculative tasks as _complete_.
		if (tip.isComplete()) {
			// Mark this task as KILLED
			tip.alreadyCompletedTask(taskid);

			// Let the JobTracker cleanup this taskid if the job isn't running
			if (this.status.getRunState() != JobStatus.RUNNING) {
				jobtracker.markCompletedTaskAttempt(status.getTaskTracker(),
						taskid);
			}
			return false;
		}

		LOG.info("Task '" + taskid + "' has completed " + tip.getTIPId()
				+ " successfully.");

		// Mark the TIP as complete
		tip.completed(taskid);
		resourceEstimator.updateWithCompletedTask(status, tip);

		// Update jobhistory
		TaskTrackerStatus ttStatus = this.jobtracker.getTaskTracker(status
				.getTaskTracker());
		String trackerHostname = jobtracker.getNode(ttStatus.getHost())
				.toString();
		String taskType = getTaskType(tip);
		if (status.getIsMap()) {
			JobHistory.MapAttempt.logStarted(status.getTaskID(),
					status.getStartTime(), status.getTaskTracker(),
					ttStatus.getHttpPort(), taskType);
			JobHistory.MapAttempt.logFinished(status.getTaskID(),
					status.getFinishTime(), trackerHostname, taskType,
					status.getStateString(), status.getCounters());

		} else {
			JobHistory.ReduceAttempt.logStarted(status.getTaskID(),
					status.getStartTime(), status.getTaskTracker(),
					ttStatus.getHttpPort(), taskType);
			JobHistory.ReduceAttempt.logFinished(status.getTaskID(),
					status.getShuffleFinishTime(), status.getSortFinishTime(),
					status.getFinishTime(), trackerHostname, taskType,
					status.getStateString(), status.getCounters());
		}
		JobHistory.Task.logFinished(tip.getTIPId(), taskType,
				tip.getExecFinishTime(), status.getCounters());

		int newNumAttempts = tip.getActiveTasks().size();
		if (tip.isJobSetupTask()) {
			// setup task has finished. kill the extra setup tip
			killSetupTip(!tip.isMapTask());
			// Job can start running now.
			this.status.setSetupProgress(1.0f);
			// move the job to running state if the job is in prep state
			if (this.status.getRunState() == JobStatus.PREP) {
				this.status.setRunState(JobStatus.RUNNING);
				JobHistory.JobInfo.logStarted(profile.getJobID());
			}
		} else if (tip.isJobCleanupTask()) {
			// cleanup task has finished. Kill the extra cleanup tip
			if (tip.isMapTask()) {
				// kill the reduce tip
				cleanup[1].kill();
			} else {
				cleanup[0].kill();
			}
			//
			// The Job is done
			// if the job is failed, then mark the job failed.
			if (jobFailed) {
				terminateJob(JobStatus.FAILED);
			}
			// if the job is killed, then mark the job killed.
			if (jobKilled) {
				terminateJob(JobStatus.KILLED);
			} else {
				System.out.println("call job complete:");
				jobComplete();
			}

		} else if (tip.isMapTask()) {

			try {
				addScheduleHistory(ttStatus.getHost(), tip.getIdWithinJob(),
						tip.getSuccessfulTaskid());
			} catch (IOException e) {
				e.printStackTrace();
			}
			runningMapTasks -= 1;
			// check if this was a sepculative task
			if (oldNumAttempts > 1) {
				speculativeMapTasks -= (oldNumAttempts - newNumAttempts);
			}
			finishedMapTasks += 1;
			metrics.completeMap(taskid);
			// remove the completed map from the resp running caches
			retireMap(tip);
			if ((finishedMapTasks + failedMapTIPs) == (numMapTasks)) {
				this.status.setMapProgress(1.0f);

				// Yingyi:
				if (numReduceTasks == 0) {
					// System.out.println("start new: " + currentIteration);
					this.startNewIteration();
				}
			}
		} else {
			if (conf.isIterative()
					&& (loopReduceCacheSwitch.isCacheWritten(conf,
							conf.getCurrentIteration(), conf.getCurrentStep()))
					|| loopReduceCacheSwitch.isCacheRead(conf,
							conf.getCurrentIteration(), conf.getCurrentStep())
					|| loopReduceOutputCacheSwitch.isCacheRead(conf,
							conf.getCurrentIteration(), conf.getCurrentStep())
					|| loopReduceOutputCacheSwitch.isCacheWritten(conf,
							conf.getCurrentIteration(), conf.getCurrentStep())) {
				// record reduce schedule history
				List<Integer> reduceList = taskTrackerToReduceTask.get(ttStatus
						.getHost());

				if (reduceList == null) {
					reduceList = new ArrayList<Integer>();
					taskTrackerToReduceTask.put(ttStatus.getHost(), reduceList);
				}
				int target = tip.getIdWithinJob();
				System.out.println("completed " + target + " "
						+ ttStatus.getHost());
				reduceList.add(target);
				reducePartitionToTts.put(target, ttStatus.getHost());
			}

			runningReduceTasks -= 1;
			if (oldNumAttempts > 1) {
				speculativeReduceTasks -= (oldNumAttempts - newNumAttempts);
			}
			finishedReduceTasks += 1;
			metrics.completeReduce(taskid);
			// remove the completed reduces from the running reducers set
			retireReduce(tip);
			if ((finishedReduceTasks + failedReduceTIPs) == (numReduceTasks)) {
				this.status.setReduceProgress(1.0f);
				if (conf.isIterative())
					loopStepHook.postStep(conf, conf.getCurrentIteration(),
							conf.getCurrentStep());
				// Yingyi:
				startNewIteration();
			}
		}

		return true;
	}

	/**
	 * start a new iteration after one iteration is completed
	 */
	private void startNewIteration() {
		boolean iterative = conf.isIterative();
		System.out.println("iterative: " + iterative);
		if (!iterative)
			return;

		System.out.println("Iteration " + currentRounds + "finished!");

		System.out.println("current iteration " + currentRounds
				+ "  specified iteration " + specifiedRounds);
		/**
		 * Yingyi: start a new iteration if there is any
		 */
		if (conf.isIterative() && conf.getFixpointCheck()
				&& conf.getCurrentStep() == conf.getNumberOfLoopBodySteps() - 1) {
			if (!checker.IsFixedPoint(conf, jobId, conf.getCurrentIteration(),
					conf.getCurrentStep())) {
				currentRounds++;
				System.out.println("current iteration " + currentRounds
						+ "  specified iteration " + specifiedRounds);
				try {
					iterationInitTasks();

					mapScheduled = new boolean[this.numMapTasks];
					for (int i = 0; i < mapScheduled.length; i++)
						mapScheduled[i] = false;

					reduceScheduled = new boolean[this.numReduceTasks];
					for (int i = 0; i < reduceScheduled.length; i++)
						reduceScheduled[i] = false;

				} catch (Exception e) {
					LOG.debug(e.getStackTrace());
					System.out.println(e.getLocalizedMessage());
					e.printStackTrace();
				}
				System.out.println("Current Iteration after process: "
						+ currentRounds);
			} else {
				canExist = true;
			}
		} else if (conf.isIterative() && specifiedRounds > 0
				&& currentRounds < specifiedRounds - 1) {
			currentRounds++;
			System.out.println("current iteration " + currentRounds
					+ "  specified iteration " + specifiedRounds);
			try {
				iterationInitTasks();

				mapScheduled = new boolean[this.numMapTasks];
				for (int i = 0; i < mapScheduled.length; i++)
					mapScheduled[i] = false;

				reduceScheduled = new boolean[this.numReduceTasks];
				for (int i = 0; i < reduceScheduled.length; i++)
					reduceScheduled[i] = false;

			} catch (Exception e) {
				LOG.debug(e.getStackTrace());
				System.out.println(e.getLocalizedMessage());
				e.printStackTrace();
			}
			System.out.println("Current Iteration after process: "
					+ currentRounds);
		} else {
			canExist = true;
		}
	}

	/**
	 * The job is done since all it's component tasks are either successful or
	 * have failed.
	 */
	private void jobComplete() {
		System.out.println("Job complete is called!");
		final JobTrackerInstrumentation metrics = jobtracker
				.getInstrumentation();
		//
		// All tasks are complete, then the job is done!
		//
		if (this.status.getRunState() == JobStatus.RUNNING) {
			this.status.setRunState(JobStatus.SUCCEEDED);
			this.status.setCleanupProgress(1.0f);
			if (maps.length == 0) {
				this.status.setMapProgress(1.0f);
			}
			if (reduces.length == 0) {
				this.status.setReduceProgress(1.0f);
			}
			this.finishTime = System.currentTimeMillis();
			LOG.info("Job " + this.status.getJobID()
					+ " has completed successfully.");
			JobHistory.JobInfo.logFinished(this.status.getJobID(), finishTime,
					this.finishedMapTasks, this.finishedReduceTasks,
					failedMapTasks, failedReduceTasks, getCounters());
			// Note that finalize will close the job history handles which
			// garbage collect
			// might try to finalize
			garbageCollect();

			metrics.completeJob(this.conf, this.status.getJobID());

			/**
			 * HaLoop: close sheduling log
			 */
			if (scheduleLog != null) {
				try {
					scheduleLog.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private synchronized void terminateJob(int jobTerminationState) {
		if ((status.getRunState() == JobStatus.RUNNING)
				|| (status.getRunState() == JobStatus.PREP)) {
			if (jobTerminationState == JobStatus.FAILED) {
				this.status = new JobStatus(status.getJobID(), 1.0f, 1.0f,
						1.0f, JobStatus.FAILED, status.getJobPriority());
				this.finishTime = System.currentTimeMillis();
				JobHistory.JobInfo.logFailed(this.status.getJobID(),
						finishTime, this.finishedMapTasks,
						this.finishedReduceTasks);
			} else {
				this.status = new JobStatus(status.getJobID(), 1.0f, 1.0f,
						1.0f, JobStatus.KILLED, status.getJobPriority());
				this.finishTime = System.currentTimeMillis();
				JobHistory.JobInfo.logKilled(this.status.getJobID(),
						finishTime, this.finishedMapTasks,
						this.finishedReduceTasks);
			}
			garbageCollect();
			jobtracker.getInstrumentation().terminateJob(this.conf,
					this.status.getJobID());
		}
	}

	/**
	 * Terminate the job and all its component tasks. Calling this will lead to
	 * marking the job as failed/killed. Cleanup tip will be launched. If the
	 * job has not inited, it will directly call terminateJob as there is no
	 * need to launch cleanup tip. This method is reentrant.
	 * 
	 * @param jobTerminationState
	 *            job termination state
	 */
	private synchronized void terminate(int jobTerminationState) {
		if (!tasksInited.get()) {
			// init could not be done, we just terminate directly.
			terminateJob(jobTerminationState);
			return;
		}

		if ((status.getRunState() == JobStatus.RUNNING)
				|| (status.getRunState() == JobStatus.PREP)) {
			LOG.info("Killing job '" + this.status.getJobID() + "'");
			if (jobTerminationState == JobStatus.FAILED) {
				if (jobFailed) {// reentrant
					return;
				}
				jobFailed = true;
			} else if (jobTerminationState == JobStatus.KILLED) {
				if (jobKilled) {// reentrant
					return;
				}
				System.out.println("job has been killed!!!");
				jobKilled = true;
			}
			// clear all unclean tasks
			clearUncleanTasks();
			//
			// kill all TIPs.
			//
			for (int i = 0; i < setup.length; i++) {
				setup[i].kill();
			}
			for (int i = 0; i < maps.length; i++) {
				maps[i].kill();
			}
			for (int i = 0; i < reduces.length; i++) {
				reduces[i].kill();
			}
		}
	}

	private void clearUncleanTasks() {
		TaskAttemptID taskid = null;
		TaskInProgress tip = null;
		while (!mapCleanupTasks.isEmpty()) {
			taskid = mapCleanupTasks.remove(0);
			tip = maps[taskid.getTaskID().getId()];
			updateTaskStatus(tip, tip.getTaskStatus(taskid));
		}
		while (!reduceCleanupTasks.isEmpty()) {
			taskid = reduceCleanupTasks.remove(0);
			tip = reduces[taskid.getTaskID().getId()];
			updateTaskStatus(tip, tip.getTaskStatus(taskid));
		}
	}

	/**
	 * Kill the job and all its component tasks. This method should be called
	 * from jobtracker and should return fast as it locks the jobtracker.
	 */
	public void kill() {
		boolean killNow = false;
		synchronized (jobInitKillStatus) {
			jobInitKillStatus.killed = true;
			// if not in middle of init, terminate it now
			if (!jobInitKillStatus.initStarted || jobInitKillStatus.initDone) {
				// avoiding nested locking by setting flag
				killNow = true;
			}
		}
		if (killNow) {
			terminate(JobStatus.KILLED);
		}
	}

	/**
	 * Fails the job and all its component tasks. This should be called only
	 * from {@link JobInProgress} or {@link JobTracker}. Look at
	 * {@link JobTracker#failJob(JobInProgress)} for more details.
	 */
	synchronized void fail() {
		terminate(JobStatus.FAILED);
	}

	/**
	 * A task assigned to this JobInProgress has reported in as failed. Most of
	 * the time, we'll just reschedule execution. However, after many repeated
	 * failures we may instead decide to allow the entire job to fail or succeed
	 * if the user doesn't care about a few tasks failing.
	 * 
	 * Even if a task has reported as completed in the past, it might later be
	 * reported as failed. That's because the TaskTracker that hosts a map task
	 * might die before the entire job can complete. If that happens, we need to
	 * schedule reexecution so that downstream reduce tasks can obtain the map
	 * task's output.
	 */
	private void failedTask(TaskInProgress tip, TaskAttemptID taskid,
			TaskStatus status, TaskTrackerStatus taskTrackerStatus,
			boolean wasRunning, boolean wasComplete) {
		final JobTrackerInstrumentation metrics = jobtracker
				.getInstrumentation();
		// check if the TIP is already failed
		boolean wasFailed = tip.isFailed();

		// Mark the taskid as FAILED or KILLED
		tip.incompleteSubTask(taskid, this.status);

		boolean isRunning = tip.isRunning();
		boolean isComplete = tip.isComplete();

		// update running count on task failure.
		if (wasRunning && !isRunning) {
			if (tip.isJobCleanupTask()) {
				launchedCleanup = false;
			} else if (tip.isJobSetupTask()) {
				launchedSetup = false;
			} else if (tip.isMapTask()) {
				runningMapTasks -= 1;
				metrics.failedMap(taskid);
				// remove from the running queue and put it in the non-running
				// cache
				// if the tip is not complete i.e if the tip still needs to be
				// run
				if (!isComplete) {
					retireMap(tip);
					failMap(tip, taskTrackerStatus);
				}
			} else {
				runningReduceTasks -= 1;
				metrics.failedReduce(taskid);
				// remove from the running queue and put in the failed queue if
				// the tip
				// is not complete
				if (!isComplete) {
					retireReduce(tip);
					failReduce(tip, taskTrackerStatus);
				}
			}
		}

		// the case when the map was complete but the task tracker went down.
		if (wasComplete && !isComplete) {
			if (tip.isMapTask()) {
				// Put the task back in the cache. This will help locality for
				// cases
				// where we have a different TaskTracker from the same
				// rack/switch
				// asking for a task.
				// We bother about only those TIPs that were successful
				// earlier (wasComplete and !isComplete)
				// (since they might have been removed from the cache of other
				// racks/switches, if the input split blocks were present there
				// too)
				failMap(tip, taskTrackerStatus);
				finishedMapTasks -= 1;
			}
		}

		// update job history
		// get taskStatus from tip
		TaskStatus taskStatus = tip.getTaskStatus(taskid);
		String taskTrackerName = taskStatus.getTaskTracker();
		String taskTrackerHostName = convertTrackerNameToHostName(taskTrackerName);
		int taskTrackerPort = -1;
		if (taskTrackerStatus != null) {
			taskTrackerPort = taskTrackerStatus.getHttpPort();
		}
		long startTime = taskStatus.getStartTime();
		long finishTime = taskStatus.getFinishTime();
		List<String> taskDiagnosticInfo = tip.getDiagnosticInfo(taskid);
		String diagInfo = taskDiagnosticInfo == null ? "" : StringUtils
				.arrayToString(taskDiagnosticInfo.toArray(new String[0]));
		String taskType = getTaskType(tip);
		if (taskStatus.getIsMap()) {
			JobHistory.MapAttempt.logStarted(taskid, startTime,
					taskTrackerName, taskTrackerPort, taskType);
			if (taskStatus.getRunState() == TaskStatus.State.FAILED) {
				JobHistory.MapAttempt.logFailed(taskid, finishTime,
						taskTrackerHostName, diagInfo, taskType);
			} else {
				JobHistory.MapAttempt.logKilled(taskid, finishTime,
						taskTrackerHostName, diagInfo, taskType);
			}
		} else {
			JobHistory.ReduceAttempt.logStarted(taskid, startTime,
					taskTrackerName, taskTrackerPort, taskType);
			if (taskStatus.getRunState() == TaskStatus.State.FAILED) {
				JobHistory.ReduceAttempt.logFailed(taskid, finishTime,
						taskTrackerHostName, diagInfo, taskType);
			} else {
				JobHistory.ReduceAttempt.logKilled(taskid, finishTime,
						taskTrackerHostName, diagInfo, taskType);
			}
		}

		// After this, try to assign tasks with the one after this, so that
		// the failed task goes to the end of the list.
		if (!tip.isJobCleanupTask() && !tip.isJobSetupTask()) {
			if (tip.isMapTask()) {
				failedMapTasks++;
			} else {
				failedReduceTasks++;
			}
		}

		//
		// Note down that a task has failed on this tasktracker
		//
		if (status.getRunState() == TaskStatus.State.FAILED) {
			addTrackerTaskFailure(taskTrackerName);
		}

		//
		// Let the JobTracker know that this task has failed
		//
		jobtracker.markCompletedTaskAttempt(status.getTaskTracker(), taskid);

		//
		// Check if we need to kill the job because of too many failures or
		// if the job is complete since all component tasks have completed

		// We do it once per TIP and that too for the task that fails the TIP
		if (!wasFailed && tip.isFailed()) {
			//
			// Allow upto 'mapFailuresPercent' of map tasks to fail or
			// 'reduceFailuresPercent' of reduce tasks to fail
			//
			boolean killJob = tip.isJobCleanupTask() || tip.isJobSetupTask() ? true
					: tip.isMapTask() ? ((++failedMapTIPs * 100) > (mapFailuresPercent * numMapTasks))
							: ((++failedReduceTIPs * 100) > (reduceFailuresPercent * numReduceTasks));

			if (killJob) {
				LOG.info("Aborting job " + profile.getJobID());
				JobHistory.Task.logFailed(tip.getTIPId(), taskType, finishTime,
						diagInfo);
				if (tip.isJobCleanupTask()) {
					// kill the other tip
					if (tip.isMapTask()) {
						cleanup[1].kill();
					} else {
						cleanup[0].kill();
					}
					terminateJob(JobStatus.FAILED);
				} else {
					if (tip.isJobSetupTask()) {
						// kill the other tip
						killSetupTip(!tip.isMapTask());
					}
					fail();
				}
			}

			//
			// Update the counters
			//
			if (!tip.isJobCleanupTask() && !tip.isJobSetupTask()) {
				if (tip.isMapTask()) {
					jobCounters.incrCounter(Counter.NUM_FAILED_MAPS, 1);
				} else {
					jobCounters.incrCounter(Counter.NUM_FAILED_REDUCES, 1);
				}
			}
		}
	}

	/**
	 * clear tasks on failed tracker
	 * 
	 * @param trackerName
	 */
	public void clearTasksOnFailedTracker(String trackerName) {
		String[] strs = trackerName.split(":");
		String[] trackers = strs[0].split("_");
		String host = trackers[1];

		System.out.println("HaLoop notice tracker " + trackerName + "failed");

		List<Integer> mapList = this.taskTrackerToMapTask.get(host);
		if (mapList != null) {
			int i = mapList.size() - 1;
			for (; i >= 0; i--) {
				int target = mapList.remove(i);
				isSpeculativeMap[target] = true;
			}
		}

		List<Integer> reduceList = this.taskTrackerToReduceTask.get(host);
		if (reduceList != null) {
			int i = reduceList.size() - 1;
			for (; i >= 0; i--) {
				int target = reduceList.remove(i);
				isSpeculativeReduce[target] = true;
			}
		}
	}

	void killSetupTip(boolean isMap) {
		if (isMap) {
			setup[0].kill();
		} else {
			setup[1].kill();
		}
	}

	boolean isSetupFinished() {
		if (setup[0].isComplete() || setup[0].isFailed()
				|| setup[1].isComplete() || setup[1].isFailed()) {
			return true;
		}
		return false;
	}

	/**
	 * Fail a task with a given reason, but without a status object.
	 * 
	 * Assuming {@link JobTracker} is locked on entry.
	 * 
	 * @param tip
	 *            The task's tip
	 * @param taskid
	 *            The task id
	 * @param reason
	 *            The reason that the task failed
	 * @param trackerName
	 *            The task tracker the task failed on
	 */
	public void failedTask(TaskInProgress tip, TaskAttemptID taskid,
			String reason, TaskStatus.Phase phase, TaskStatus.State state,
			String trackerName) {
		TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(),
				taskid, 0.0f, state, reason, reason, trackerName, phase,
				new Counters());
		// update the actual start-time of the attempt
		TaskStatus oldStatus = tip.getTaskStatus(taskid);
		long startTime = oldStatus == null ? System.currentTimeMillis()
				: oldStatus.getStartTime();
		status.setStartTime(startTime);
		status.setFinishTime(System.currentTimeMillis());
		boolean wasComplete = tip.isComplete();
		updateTaskStatus(tip, status);
		boolean isComplete = tip.isComplete();
		if (wasComplete && !isComplete) { // mark a successful tip as failed
			String taskType = getTaskType(tip);
			JobHistory.Task.logFailed(tip.getTIPId(), taskType,
					tip.getExecFinishTime(), reason, taskid);
		}
	}

	/**
	 * The job is dead. We're now GC'ing it, getting rid of the job from all
	 * tables. Be sure to remove all of this job's tasks from the various
	 * tables.
	 */
	synchronized void garbageCollect() {
		// Let the JobTracker know that a job is complete
		jobtracker.getInstrumentation().decWaiting(getJobID(),
				pendingMaps() + pendingReduces());
		jobtracker.storeCompletedJob(this);
		jobtracker.finalizeJob(this);

		try {
			// Definitely remove the local-disk copy of the job file
			if (localJobFile != null) {
				localFs.delete(localJobFile, true);
				localJobFile = null;
			}
			if (localJarFile != null) {
				localFs.delete(localJarFile, true);
				localJarFile = null;
			}

			// clean up splits
			for (int i = 0; i < maps.length; i++) {
				maps[i].clearSplit();
			}

			// JobClient always creates a new directory with job files
			// so we remove that directory to cleanup
			// Delete temp dfs dirs created if any, like in case of
			// speculative exn of reduces.
			Path tempDir = jobtracker.getSystemDirectoryForJob(getJobID());
			new CleanupQueue().addToQueue(conf, tempDir);
		} catch (IOException e) {
			LOG.warn("Error cleaning up " + profile.getJobID() + ": " + e);
		}

		cleanUpMetrics();
		// free up the memory used by the data structures
		this.nonRunningMapCache = null;
		this.runningMapCache = null;
		this.nonRunningReduces = null;
		this.runningReduces = null;

	}

	/**
	 * Return the TaskInProgress that matches the tipid.
	 */
	public synchronized TaskInProgress getTaskInProgress(TaskID tipid) {
		if (tipid.isMap()) {
			if (tipid.equals(cleanup[0].getTIPId())) { // cleanup map tip
				return cleanup[0];
			}
			if (tipid.equals(setup[0].getTIPId())) { // setup map tip
				return setup[0];
			}
			for (int i = 0; i < maps.length; i++) {
				if (tipid.equals(maps[i].getTIPId())) {
					return maps[i];
				}
			}
		} else {
			if (tipid.equals(cleanup[1].getTIPId())) { // cleanup reduce tip
				return cleanup[1];
			}
			if (tipid.equals(setup[1].getTIPId())) { // setup reduce tip
				return setup[1];
			}
			for (int i = 0; i < reduces.length; i++) {
				if (tipid.equals(reduces[i].getTIPId())) {
					return reduces[i];
				}
			}
		}
		return null;
	}

	/**
	 * Find the details of someplace where a map has finished
	 * 
	 * @param mapId
	 *            the id of the map
	 * @return the task status of the completed task
	 */
	public synchronized TaskStatus findFinishedMap(int mapId) {
		TaskInProgress tip = maps[mapId];
		if (tip.isComplete()) {
			TaskStatus[] statuses = tip.getTaskStatuses();
			for (int i = 0; i < statuses.length; i++) {
				if (statuses[i].getRunState() == TaskStatus.State.SUCCEEDED) {
					return statuses[i];
				}
			}
		}
		return null;
	}

	synchronized int getNumTaskCompletionEvents() {
		return taskCompletionEvents.size();
	}

	synchronized public TaskCompletionEvent[] getTaskCompletionEvents(
			int fromEventId, int maxEvents) {
		TaskCompletionEvent[] events = TaskCompletionEvent.EMPTY_ARRAY;
		if (taskCompletionEvents.size() > fromEventId) {
			int actualMax = Math.min(maxEvents,
					(taskCompletionEvents.size() - fromEventId));
			events = taskCompletionEvents.subList(fromEventId,
					actualMax + fromEventId).toArray(events);
		}

		System.out.println("get events from " + fromEventId);
		for (int i = 0; i < events.length; i++)
			System.out.println(events[i].getTaskTrackerHttp());

		return events;
	}

	synchronized void fetchFailureNotification(TaskInProgress tip,
			TaskAttemptID mapTaskId, String trackerName) {
		Integer fetchFailures = mapTaskIdToFetchFailuresMap.get(mapTaskId);
		fetchFailures = (fetchFailures == null) ? 1 : (fetchFailures + 1);
		mapTaskIdToFetchFailuresMap.put(mapTaskId, fetchFailures);
		LOG.info("Failed fetch notification #" + fetchFailures + " for task "
				+ mapTaskId);

		float failureRate = (float) fetchFailures / runningReduceTasks;
		// declare faulty if fetch-failures >= max-allowed-failures
		boolean isMapFaulty = (failureRate >= MAX_ALLOWED_FETCH_FAILURES_PERCENT) ? true
				: false;
		if (fetchFailures >= MAX_FETCH_FAILURES_NOTIFICATIONS && isMapFaulty) {
			LOG.info("Too many fetch-failures for output of task: " + mapTaskId
					+ " ... killing it");

			failedTask(tip, mapTaskId, "Too many fetch-failures",
					(tip.isMapTask() ? TaskStatus.Phase.MAP
							: TaskStatus.Phase.REDUCE),
					TaskStatus.State.FAILED, trackerName);

			mapTaskIdToFetchFailuresMap.remove(mapTaskId);
		}
	}

	/**
	 * @return The JobID of this JobInProgress.
	 */
	public JobID getJobID() {
		return jobId;
	}

	public synchronized Object getSchedulingInfo() {
		return this.schedulingInfo;
	}

	public synchronized void setSchedulingInfo(Object schedulingInfo) {
		this.schedulingInfo = schedulingInfo;
		this.status.setSchedulingInfo(schedulingInfo.toString());
	}

	/**
	 * To keep track of kill and initTasks status of this job. initTasks() take
	 * a lock on JobInProgress object. kill should avoid waiting on
	 * JobInProgress lock since it may take a while to do initTasks().
	 */
	private static class JobInitKillStatus {
		// flag to be set if kill is called
		boolean killed;

		boolean initStarted;
		boolean initDone;
	}

	boolean isComplete() {
		return status.isJobComplete();
	}

	/**
	 * Get the task type for logging it to {@link JobHistory}.
	 */
	private String getTaskType(TaskInProgress tip) {
		if (tip.isJobCleanupTask()) {
			return Values.CLEANUP.name();
		} else if (tip.isJobSetupTask()) {
			return Values.SETUP.name();
		} else if (tip.isMapTask()) {
			return Values.MAP.name();
		} else {
			return Values.REDUCE.name();
		}
	}
}
