package org.apache.hadoop.examples.kmeans;

import org.apache.hadoop.mapred.iterative.LoopMapCacheFilter;

public class KMeansLoopMapCacheFilter implements LoopMapCacheFilter {

	@Override
	public boolean isCache(Object key, Object value, int count) {
		return true;
	}

}
