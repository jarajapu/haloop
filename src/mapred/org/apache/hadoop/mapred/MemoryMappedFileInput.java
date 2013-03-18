package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class MemoryMappedFileInput extends InputStream implements DataInput,
		Seekable, PositionedReadable {

	private byte[] buffer = null;

	private int length = 0;

	private int position = 0;

	public MemoryMappedFileInput(byte[] bb) {
		buffer = bb;
		length = bb.length;
	}

	@Override
	public int available() throws IOException {
		return (length - position);
	}

	@Override
	public boolean readBoolean() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int read() throws IOException {
		int i = buffer[position];
		position++;
		return i;
	}

	@Override
	public byte readByte() throws IOException {
		// TODO Auto-generated method stub
		byte i = buffer[position];
		position++;
		return i;
	}

	@Override
	public char readChar() throws IOException {
		// TODO Auto-generated method stub
		return '0';
	}

	@Override
	public double readDouble() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float readFloat() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void readFully(byte[] arg0) throws IOException {
		// TODO Auto-generated method stub
		System.arraycopy(buffer, position, arg0, 0, arg0.length);
		position += arg0.length;
	}

	@Override
	public void readFully(byte[] arg0, int arg1, int arg2) throws IOException {
		// TODO Auto-generated method stub
		System.arraycopy(buffer, position, arg0, arg1, arg2);
		position += arg2;
	}

	@Override
	public void readFully(long pos, byte[] arg0, int arg1, int arg2)
			throws IOException {
		// TODO Auto-generated method stub
		position = (int) pos;
		System.arraycopy(buffer, position, arg0, arg1, arg2);
		position += arg2;
	}

	@Override
	public void readFully(long pos, byte[] arg0) throws IOException {
		// TODO Auto-generated method stub
		readFully(pos, arg0, 0, arg0.length);
	}

	@Override
	public int read(byte[] arg0, int arg1, int arg2) throws IOException {
		// TODO Auto-generated method stub
		readFully(arg0, arg1, arg2);
		return arg2;
	}

	@Override
	public int read(long pos, byte[] arg0, int arg1, int arg2)
			throws IOException {
		// TODO Auto-generated method stub
		readFully(pos, arg0, arg1, arg2);
		return arg2;
	}

	@Override
	public int readInt() throws IOException {
		// TODO Auto-generated method stub
		int value = arr2Int(buffer, position);
		position += 4;
		return value;
	}

	@Override
	public String readLine() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long readLong() throws IOException {
		// TODO Auto-generated method stub
		long value = arr2Long(buffer, position);
		position += 8;
		return value;
	}

	@Override
	public short readShort() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String readUTF() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int readUnsignedByte() throws IOException {
		// TODO Auto-generated method stub
		return readByte();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		// TODO Auto-generated method stub
		return readInt();
	}

	@Override
	public int skipBytes(int n) throws IOException {
		// TODO Auto-generated method stub
		position += n;
		return n;
	}

	public void seek(int pos) throws IOException {
		position = pos;
	}

	@Override
	public void seek(long pos) throws IOException {
		position = (int) pos;
	}

	@Override
	public boolean seekToNewSource(long pos) throws IOException {
		return false;
	}

	@Override
	public long getPos() throws IOException {
		return position;
	}

	public int length() {
		return length;
	}

	@Override
	public void close() {
		buffer = null;
		length = -1;
	}

	private static long arr2Long(byte[] arr, int start) {
		int i = 0;
		int len = 8;
		int cnt = 0;
		byte[] tmp = new byte[len];
		for (i = start; i < (start + len); i++) {
			tmp[cnt] = arr[i];
			cnt++;
		}
		long accum = 0;
		i = 0;
		for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
			accum |= ((long) (tmp[i] & 0xff)) << shiftBy;
			i++;
		}
		return accum;
	}
	
	private static int arr2Int(byte[] arr, int start) {
		int i = 0;
		int len = 4;
		int cnt = 0;
		byte[] tmp = new byte[len];
		for (i = start; i < (start + len); i++) {
			tmp[cnt] = arr[i];
			cnt++;
		}
		int accum = 0;
		i = 0;
		for (int shiftBy = 0; shiftBy < 32; shiftBy += 8) {
			accum |= ((int) (tmp[i] & 0xff)) << shiftBy;
			i++;
		}
		return accum;
	}
}
