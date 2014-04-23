package org.wiperdog.jobmanager.internal;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.wiperdog.jobmanager.Constants;
import org.wiperdog.jobmanager.JobExecutable;


@DisallowConcurrentExecution
public class ObjectJob extends AbstractGenericJob {
	JobDataMap data = null;
	Thread me = null;
	private JobExecutable getExecutable(JobDataMap datamap) {
		return (JobExecutable) data.get(Constants.KEY_OBJECT);
	}
	
	@Override
	protected Object doJob(JobExecutionContext context) throws Throwable {
		JobDataMap data = context.getMergedJobDataMap();
		this.data = data;
		me = Thread.currentThread();
		
		JobExecutable exe = null;
		Boolean value = Boolean.FALSE;
		try {
			exe = getExecutable(data);
		} catch (ClassCastException e) {
		}
		if (exe != null) {
			Object result = null;
			try {
				result = exe.execute(data);
			} catch (InterruptedException e) {
				throw e;
			}
			if (result == null) {
				//  execution ended with FALSE value
			} else {
				value = Boolean.parseBoolean(result.toString());
			}
		}
		this.data = null;
		return value;
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		if (data != null) {
			JobExecutable exe = getExecutable(data);
			if (exe != null) {
				exe.stop(me);
			}
		}
	}	
}