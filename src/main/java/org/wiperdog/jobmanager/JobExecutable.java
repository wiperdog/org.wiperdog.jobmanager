package org.wiperdog.jobmanager;

import org.quartz.*;

/**
 * Interface for executable job.
 */
public interface JobExecutable {

	/**
	 * Executing the job
	 * @param params
	 */
	Object execute(JobDataMap params)throws InterruptedException;

	/**
	 * Get name used to uniquely recognize this job.
	 */
	String getName();

	String getArgumentString();

	/**
	 * Stop execution immediately. This should be invoked by InterruptableJob.interrupt() method.  
	 */
	void stop(Thread t);
}