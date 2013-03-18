package org.apache.hadoop.mapred.iterative;

public class DefaultDistanceMeasure<K, V> implements IDistanceMeasure<K, V> {

	@Override
	public float getDistance(K key1, V value1, K key2, V value2) {
		return Float.MAX_VALUE;
	}

}
