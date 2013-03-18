package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * reconstructed map schedule info from jobtracker's scheduling log on the
 * 
 * @author yingyib
 */
class MapScheduleInfo implements Writable {

	private String httpAddress;
	private int partition;
	private JobClient.RawSplit inputSplit;

	private TaskAttemptID tid;

	public MapScheduleInfo() {
		inputSplit = new JobClient.RawSplit();
		tid = new TaskAttemptID();
	}

	public MapScheduleInfo(String httpAddress, int part,
			JobClient.RawSplit inputSplit, TaskAttemptID tid) {
		this.httpAddress = httpAddress;
		this.partition = part;
		this.inputSplit = inputSplit;
		this.tid = tid;
	}

	public boolean equal(Object object) {
		MapScheduleInfo msi = (MapScheduleInfo) object;
		if (this.partition == msi.partition
				&& httpAddress.equals(msi.httpAddress) && tid.equals(msi.tid)) {
			return true;
		} else
			return false;
	}

	public String getHttpHost() {
		return httpAddress;
	}

	public void setHttpAddress(String addr) {
		httpAddress = addr;
	}

	public JobClient.RawSplit getInputSplit() {
		return inputSplit;
	}

	public int getPartition() {
		return partition;
	}

	public TaskAttemptID getTaskAttemptID() {
		return tid;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, httpAddress);
		out.writeInt(partition);
		inputSplit.write(out);
		tid.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		httpAddress = Text.readString(in);
		partition = in.readInt();
		inputSplit.readFields(in);
		tid.readFields(in);
	}
}