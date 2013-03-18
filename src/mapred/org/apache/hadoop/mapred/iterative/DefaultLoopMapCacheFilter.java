package org.apache.hadoop.mapred.iterative;

public class DefaultLoopMapCacheFilter implements LoopMapCacheFilter {

	@Override
	public boolean isCache(Object key, Object value, int count) {
		return false;
	}

}
