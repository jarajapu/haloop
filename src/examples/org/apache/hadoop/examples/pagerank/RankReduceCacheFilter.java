package org.apache.hadoop.examples.pagerank;

import org.apache.hadoop.mapred.iterative.LoopReduceCacheFilter;

public class RankReduceCacheFilter implements LoopReduceCacheFilter {

	@Override
	public boolean isCache(Object key, Object value, int count) {
		if (count <= 1)
			return false;
		else
			return true;
	}

}
