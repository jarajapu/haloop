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

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.iterative.LoopMapCacheFilter;
import org.apache.hadoop.mapred.iterative.LoopMapCacheSwitch;
import org.apache.hadoop.util.ReflectionUtils;

/** Default {@link MapRunnable} implementation. */
public class MapRunner<K1, V1, K2, V2> implements MapRunnable<K1, V1, K2, V2> {

	/**
	 * the file name of local mapper input cache
	 */
	private Path cacheFileName = null;

	private Mapper<K1, V1, K2, V2> mapper;
	private boolean incrProcCount;
	private JobConf conf;

	protected MapOutputFile mapOutputFile = new MapOutputFile();
	private LoopMapCacheSwitch cacheSwitch;
	private LoopMapCacheFilter cacheFilter;

	private FSDataOutputStream fileOutput = null;
	private FSDataInputStream fileInput = null;

	private boolean localTask = false;

	@SuppressWarnings("unchecked")
	public void configure(JobConf job) {
		conf = job;
		mapper = ReflectionUtils.newInstance(job.getMapperClass(), job);
		// increment processed counter only if skipping feature is enabled
		incrProcCount = SkipBadRecords.getMapperMaxSkipRecords(job) > 0
				&& SkipBadRecords.getAutoIncrMapperProcCount(job);
	}

	@SuppressWarnings("unchecked")
	public void configure(JobConf job, TaskAttemptID taskId, boolean local) {
		System.out.println("custom configure ");
		localTask = local;
		cacheSwitch = ReflectionUtils.newInstance(job.getLoopMapCacheSwitch(),
				job);
		cacheFilter = ReflectionUtils.newInstance(job.getLoopMapCacheFilter(),
				job);

		conf = job;
		mapOutputFile.setJobId(taskId.getJobID());
		mapOutputFile.setConf(job);

		mapper = ReflectionUtils.newInstance(job.getMapperClass(), job);
		// increment processed counter only if skipping feature is enabled
		incrProcCount = SkipBadRecords.getMapperMaxSkipRecords(job) > 0
				&& SkipBadRecords.getAutoIncrMapperProcCount(job);

		/**
		 * find the latest cached iteration/step
		 */
		int numSteps = job.getNumberOfLoopBodySteps();
		int cachedIteration = job.getCurrentIteration();
		int cachedStep = job.getCurrentStep();
		int round = cachedIteration * numSteps + cachedStep;
		cachedStep += 1;
		int latest;
		for (latest = round; latest >= 0; latest--) {
			if (cachedStep > 0)
				cachedStep--;
			else {
				cachedStep = numSteps - 1;
				cachedIteration--;
			}
			if (cacheSwitch.isCacheWritten(job, cachedIteration, cachedStep))
				break;
		}
		latest = cachedIteration * numSteps + cachedStep;
		System.out.println("cached pass " + latest);
		try {
			cacheFileName = mapOutputFile.getMapCacheFileForWrite(taskId, -1,
					latest);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
			Reporter reporter) throws IOException {
		long recordCount = 0;
		long start = System.currentTimeMillis();

		SerializationFactory serializationFactory = new SerializationFactory(
				conf);
		Serializer<K1> keySerializer = null;
		Serializer<V1> valueSerializer = null;
		Deserializer<K1> keyDeserializer = null;
		Deserializer<V1> valueDeserializer = null;

		int iteration = conf.getCurrentIteration();
		int step = conf.getCurrentStep();

		boolean tocache = cacheSwitch.isCacheWritten(conf, iteration, step)
				&& !localTask;
		boolean cached = cacheSwitch.isCacheRead(conf, iteration, step)
				&& !localTask;

		System.out.println("iteration " + iteration + " step " + step);
		if (tocache)
			System.out.println("to cache enabled ");
		else if (cached)
			System.out.println("cache enabled ");
		else
			System.out.println("no cache option ");

		// write data to cache
		if (tocache) {
			FileSystem localFs = FileSystem.getLocal(conf);
			FileSystem lfs = ((LocalFileSystem) localFs).getRaw();
			fileOutput = lfs.create(cacheFileName);
		}

		// use cached data
		if (cached) {
			FileSystem localFs = FileSystem.getLocal(conf);
			FileSystem lfs = ((LocalFileSystem) localFs).getRaw();

			if (lfs.exists(cacheFileName)) {
				/**
				 * we have cache to use
				 */
				fileInput = lfs.open(cacheFileName);
			} else {
				/**
				 * in case there is no cache to use
				 */
				cached = false;
			}
		}

		try {
			// allocate key & value instances that are re-used for all entries
			if (tocache) {
				// read key, value
				K1 key = input.createKey();
				V1 value = input.createValue();

				// intialize key/value serializer
				if (keySerializer == null || valueSerializer == null) {
					Class<K1> keyClass = (Class<K1>) key.getClass();
					Class<V1> valueClass = (Class<V1>) value.getClass();
					keySerializer = serializationFactory
							.getSerializer(keyClass);
					valueSerializer = serializationFactory
							.getSerializer(valueClass);
					keySerializer.open(fileOutput);
					valueSerializer.open(fileOutput);
				}

				long iototal = 0;
				long iostart = System.currentTimeMillis();
				long ioend = System.currentTimeMillis();

				while (input.next(key, value)) {
					ioend = System.currentTimeMillis();
					iototal += (ioend - iostart);
					recordCount++;

					// output to cache
					keySerializer.serialize(key);
					valueSerializer.serialize(value);

					// map pair to output
					mapper.map(key, value, output, reporter);

					if (incrProcCount) {
						reporter
								.incrCounter(
										SkipBadRecords.COUNTER_GROUP,
										SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS,
										1);
					}
					iostart = System.currentTimeMillis();
				}

				System.out.println("hadoop i/o time " + iototal + " ms");
				fileOutput.close();
			} else if (cached) {
				// do the cached work
				K1 key = input.createKey();
				V1 value = input.createValue();

				// intialize key/value serializer
				if (keyDeserializer == null || valueDeserializer == null) {
					Class<K1> keyClass = (Class<K1>) key.getClass();
					Class<V1> valueClass = (Class<V1>) value.getClass();
					keyDeserializer = serializationFactory
							.getDeserializer(keyClass);
					valueDeserializer = serializationFactory
							.getDeserializer(valueClass);
					keyDeserializer.open(fileInput);
					valueDeserializer.open(fileInput);
				}

				long localiostart = System.currentTimeMillis();
				long localioend = System.currentTimeMillis();
				long localiototal = 0;
				long mrstart = System.currentTimeMillis();
				long mrend = System.currentTimeMillis();
				long mrtotal = 0;

				while (fileInput.available() > 0) {
					/**
					 * deserialize key/value
					 */
					keyDeserializer.deserialize(key);
					valueDeserializer.deserialize(value);

					localioend = System.currentTimeMillis();
					localiototal += (localioend - localiostart);
					recordCount++;

					// map pair to output
					mrstart = System.currentTimeMillis();
					mapper.map(key, value, output, reporter);
					if (incrProcCount) {
						reporter
								.incrCounter(
										SkipBadRecords.COUNTER_GROUP,
										SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS,
										1);
					}
					mrend = System.currentTimeMillis();
					mrtotal += (mrend - mrstart);
				}

				System.out.println("haloop i/o time " + localiototal + " ms "
						+ " map function call time " + mrtotal + " ms");
				System.out.println("input read from file " + +recordCount
						+ " records");
				fileInput.close();
			} else {
				// no cache option goes here
				K1 key = input.createKey();
				V1 value = input.createValue();

				long iototal = 0;
				long iostart = System.currentTimeMillis();
				long ioend = System.currentTimeMillis();

				while (input.next(key, value)) {
					ioend = System.currentTimeMillis();
					iototal += (ioend - iostart);

					// map pair to output
					mapper.map(key, value, output, reporter);
					if (incrProcCount) {
						reporter
								.incrCounter(
										SkipBadRecords.COUNTER_GROUP,
										SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS,
										1);
					}
					iostart = System.currentTimeMillis();
				}
				System.out.println("hadoop i/o time " + iototal + " ms");
			}

			long end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) + " ms "
					+ recordCount + " records");
		} finally {
			mapper.close();
		}
		System.out.println("mapper finished!!");
	}

	protected Mapper<K1, V1, K2, V2> getMapper() {
		return mapper;
	}
}
