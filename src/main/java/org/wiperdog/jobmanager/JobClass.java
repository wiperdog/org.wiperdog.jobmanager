package org.wiperdog.jobmanager;

import java.util.List;

import org.quartz.JobKey;

/**
 * Definition of a class of jobs, all job assigned to this class will effect some configuration which are defined when creating the job-class.
 */
public interface JobClass {

	/**
	 * Get the job class name.
	 * @return jobClassName represent for this job class name
	 */
	String getName();

	/**
	 * Get the number of job concurrency applied for this job class.
	 * @return concurrency number of job can execute.
	 */
	int getConcurrency();

	/**
	 * Set the maximum job concurrency.
	 * @param concurrency
	 */
	void setConcurrency(int concurrency);

	/**
	 * Get the max runtime, that is a duration of time a job in this class
	 * can use for running before it would be interrupted.
	 * @return maxRunTime
	 */
	long getMaxRunTime();

	/**
	 * Set the maximum runtime, a duration of time that a job in this class
	 * can use for running before it would be interrupted.
	 * @param maxRunTime
	 */
	void setMaxRunTime(long maxRunTime);

	/**
	 * Get the maximum waiting time of a job in this class.
	 * When the job can not be run at its scheduled time due to the limitation of the number of concurrent.
	 * @return maxWaitTime
	 */
	long getMaxWaitTime();

	/**
	 * Set the maximum waiting time for job in this class.
	 * @param maxWaitTime
	 */
	void setMaxWaitTime(long maxWaitTime);

	/**
	 * Get the list of job which has been asigned to this job class.
	 * @return assignedList
	 */
	List<JobKey> getAssignedList();

	/**
	 * Get count of jobs which has been asigned to this job class and is running.
	 * @return count of jobs
	 */
	int getCurrentRunningCount();

	/**
	 * Add/assign a job to this job class.
	 * @param key Key job
	 */
	void addJob(JobKey key);

	/**
	 * Delete a job from this job class.
	 * @param key Key job
	 */
	void deleteJob(JobKey key);

	/**
	 * Close a job class
	 */
	public void close();
}