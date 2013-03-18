package org.apache.hadoop.mapred.iterative;

public interface LoopMapCacheFilter {

	/**
	 * descide whether the input key, value needs to be cached
	 * @param key
	 * @param value
	 * @param count how many tuples so far
	 * @return
	 */
	public boolean isCache(Object key, Object value, int count);
}
