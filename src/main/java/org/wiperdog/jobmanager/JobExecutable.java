package org.wiperdog.jobmanager;

import org.quartz.*;

/**
 * Interface for executable job.
 */
public interface JobExecutable {

	/**
	 * 
	 * @param params
	 */
	Object execute(JobDataMap params)throws InterruptedException;

	/**
	 * get name used to uniquely recognize this job.
	 */
	String getName();

	String getArgumentString();

	/**
	 * stop execution immediately.
	 */
	void stop(Thread t);
}