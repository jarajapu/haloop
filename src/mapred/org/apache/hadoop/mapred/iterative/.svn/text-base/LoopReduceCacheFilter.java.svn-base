package org.apache.hadoop.mapred.iterative;

public interface LoopReduceCacheFilter {

	/**
	 * decide whether the input key,value needs to be cached
	 * 
	 * @param key
	 * @param value
	 * @param count how many values so far within that key group
	 * count is number_of_tuples_so_far before current tuple
	 * @return
	 */
	public boolean isCache(Object key, Object value, int count);
}
