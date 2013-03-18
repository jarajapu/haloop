package org.apache.hadoop.mapred;

/**
 * this task runner is to lunch all
 * 
 * @author yingyib
 * 
 */
public class MultiThreadTaskRunner implements Runnable {

	private final Task task;
	private final JobConf job;
	private final TaskUmbilicalProtocol umbilical;

	public MultiThreadTaskRunner(JobConf job, TaskUmbilicalProtocol umbilical,
			Task task) {
		this.task = task;
		this.job = job;
		this.umbilical = umbilical;
	}

	@Override
	public void run() {
		try {
			task.run(job, umbilical);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
