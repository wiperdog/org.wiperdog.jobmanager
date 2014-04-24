package org.wiperdog.jobmanager;

import java.util.Date;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Trigger;

/**
 * Facade class of Job Manager bundle. This help to perform main function of the package.
 */
public interface JobFacade {

	/**
	 * Create the job class with specific name, max wait time, max runtime, max concurrency.
	 * @param jobClassName Name of the job class
	 * @param concurrency number of job can execute concurently
	 * @param maxWaitTime The maximum waiting time of a job in this job class. When waiting time reach this number, the job in this job class will be interrupted.
	 * @param maxRunTime The longest running time that a job in this job class can be run. When a job start running and the running time is over this number, the job will be interrupted.
	 */
	JobClass createJobClass(String jobClassName, int concurrency, long maxWaitTime, long maxRunTime) throws JobManagerException;

	/**
	 * Assign a job to an existing job class.
	 * @param jobName The name of job
	 * @param jobClassName The job class name.
	 */
	void assignJobClass(String jobName, String jobClassName) throws JobManagerException;

	/**
	 * Delete a job class.
	 * @param jobClassName The job class name.
	 */
	void deleteJobClass(String jobClassName) throws JobManagerException;

	/**
	 * Remove a job from a job class.
	 * @param jobName A job name
	 * @param jobClassName The job class name
	 */
	void revokeJobClass(String jobName, String jobClassName) throws JobManagerException;

	/**
	 * Create a JobExecutable job
	 * @param executable Executable job
	 */
	JobDetail createJob(JobExecutable executable) throws JobManagerException;


	

	/**
	 * Get created job by job name in job instance map
	 * @param name The name of job.
	 */
	JobDetail getJob(String name) throws JobManagerException;

	/**
	 * Get trigger create in JobFacade by trigger name.
	 * @param name The name of trigger
	 */
	Trigger getTrigger(String name) throws JobManagerException;

	/**
	 * Get a created job class by name
	 * @param name The name of job class.
	 */
	JobClass getJobClass(String name) throws JobManagerException;

	/**
	 * Pause the Quartz scheduler.
	 */
	void pauseSchedule() throws JobManagerException;

	/**
	 * Resume the Quartz scheduler.
	 */
	void resumSchedule() throws JobManagerException;

	/**
	 * Interrupt a job which was scheduled.
	 * @param name The name of job to be interrupted.
	 */
	boolean interruptJob(String name) throws JobManagerException;

	/**
	 * Search job classes on which the job specified by <code>name</code> was assigned
	 * @param name The name of job.
	 */
	JobClass[] findJobClassForJob(String name) throws JobManagerException;

	/**
	 * The number of jobs is running currently by scheduler
	 * @param name The name of job to be counted.
	 */
	int getJobRunningCount(String name) throws JobManagerException;

	/**
	 * revoke job from any existing job class
	 * @param jobName a job name
	 */
	void revokeJobClass(String jobName) throws JobManagerException;

	/**
	 * Creating a trigger.
	 * @param name The name of the trigger.
	 */
	Trigger createTrigger(String name) throws JobManagerException;

	/**
	 * Create Trigger with delay time
	 * @param name Name of the trigger
	 * @param delay The delay time in millisecond
	 */
	Trigger createTrigger(String name, long delay) throws JobManagerException;

	/**
	 * Create trigger with delay time and loop interval
	 * @param name Name of the trigger
	 * @param delay The delay time
	 * @param interval Loop interval
	 */
	Trigger createTrigger(String name, long delay, long interval) throws JobManagerException;

	/**
	 * Create trigger to start at a specified time in the future
	 * @param name Name of the trigger
	 * @param at At time (long) the trigger will be started
	 */
	Trigger createTrigger(String name, Date at) throws JobManagerException;

	/**
	 * Create trigger with configure to start with cron-tab
	 * @param name Name of the trigger
	 * @param crondef A String represent for cron-tab configuration.
	 */
	Trigger createTrigger(String name, String crondef) throws JobManagerException;
	
	
	/**
	 * Creating job.
	 * @param jobName The name of job will be created.
	 * @param clz Class of the job extends org.quartz.Job
	 * @param data A map contain job data.
	 */
	JobDetail createJob(String jobName, Class<? extends Job> clz, JobDataMap data) throws JobManagerException;

	/**
	 * Delete a job from scheduler and then remove it from JobFacade instance
	 * @param job A job to be removed
	 */
	void removeJob(JobDetail job) throws JobManagerException;

	/**
	 * Un-schedule a job that specified in a trigger
	 * @param trigger The trigger of un-schedule job.
	 */
	void unscheduleJob(Trigger trigger) throws JobManagerException;

	/**
	 * Schedule a job with parameters are specified in a trigger.
	 * @param job The Job to be scheduled
	 * @param trigger The trigger of job.
	 */
	void scheduleJob(JobDetail job, Trigger trigger) throws JobManagerException;

}