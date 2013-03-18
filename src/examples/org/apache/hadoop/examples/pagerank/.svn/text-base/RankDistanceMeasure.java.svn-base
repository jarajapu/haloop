package org.apache.hadoop.examples.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.iterative.IDistanceMeasure;

public class RankDistanceMeasure implements IDistanceMeasure<Text, Text> {

	@Override
	public float getDistance(Text key1, Text value1, Text key2, Text value2) {
		if (!key1.equals(key2))
			throw new IllegalStateException("mismatched keys");
		else {
			float v1 = Float.parseFloat(value1.toString());
			float v2 = Float.parseFloat(value2.toString());
			return Math.abs(v1 - v2);
		}
	}

}
