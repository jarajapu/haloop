package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.serializer.Deserializer;

public class CombineIterator<T> implements Iterator<T> {
	Iterator<T> iteratorOne;
	Iterator<T> iteratorTwo;

	Iterator<T> current;

	public CombineIterator(Iterator<T> it1, Iterator<T> it2) throws IOException {
		iteratorOne = it1;
		iteratorTwo = it2;

		current = iteratorOne;
	}

	@Override
	public T next() {
		return current.next();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			boolean next = current.hasNext();

			if (next == true)
				return next;
			else {
				if (current == iteratorOne) {
					current = iteratorTwo;
					next = current.hasNext();
				}
			}
			return next;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}
}