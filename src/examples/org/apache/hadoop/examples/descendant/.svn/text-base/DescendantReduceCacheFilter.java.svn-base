package org.apache.hadoop.examples.descendant;

import org.apache.hadoop.examples.textpair.TextPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.iterative.LoopReduceCacheFilter;

public class DescendantReduceCacheFilter implements LoopReduceCacheFilter {

	/**
	 * left table in the join
	 */
	Text tag0 = new Text("0");

	/**
	 * right table in the join
	 */
	Text tag1 = new Text("1");

	@Override
	public boolean isCache(Object key, Object value, int count) {
		TextPair tv = (TextPair) value;
		if (tv.getSecond().equals(tag0))
			return false;
		else if (tv.getSecond().equals(tag1))
			return true;
		else
			throw new IllegalStateException("Illegal state " + tv.getSecond());
	}

}
