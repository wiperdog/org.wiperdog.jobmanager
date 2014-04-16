package com.insight_tec.pi.jobmanager.internal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.PropertySettingJobFactory;
import org.wiperdog.jobmanager.JobFacade;
import org.wiperdog.jobmanager.internal.JobFacadeImpl;

import static org.junit.Assert.*;

public class SearchJobClassForJob {
	private SchedulerFactory sf;
	private Scheduler scheduler;
	JobFacade jf;
	String path = System.getProperty("user.dir");
	String jobName;
	
	@Before
	public void setup() throws Exception {
		sf = new StdSchedulerFactory();
		scheduler = sf.getScheduler();
		PropertySettingJobFactory jfactory = new PropertySettingJobFactory();
		jfactory.setWarnIfPropertyNotFound(false);
		scheduler.setJobFactory(jfactory);
		scheduler.start();
		jf = new JobFacadeImpl(scheduler);
	}
	
	@After
	public void shutdown() throws Exception {
		scheduler.shutdown();
		scheduler = null;
		sf = null;
	}
	
	/**
	 * Search job class with the job has been assigned to class 
	 * Expected: The array will be contains job class corresponding to the job.
	 */
	@Test
	public void search_job_class_for_job_01() throws Exception {
		// search job class with the job has been assigned to class
		String jcname = "class";
		int concurrency = 1;
		long maxruntime = 20000; 
		long maxwaittime = 20000;
		jobName = "jobTest1";
		jf.createJobClass(jcname, concurrency, maxruntime, maxwaittime);
		jf.assignJobClass(jobName, jcname);
		assertTrue(jf.findJobClassForJob(jobName).length > 0);
	}
	
	/**
	 * Search job class with the job is not assigned to class 
	 * Expected: The array does not contains job class corresponding to the job.
	 */
	@Test
	public void search_job_class_for_job_02() throws Exception {
		// search job class with the job is not assigned to class
		jobName = "jobTest";
		assertTrue(jf.findJobClassForJob(jobName).length == 0);
	}
	
	/**
	 * Search job class with jobName is empty or null.
	 * Expected: The array does not contains job class corresponding to the job.
	 */
	@Test
	public void search_job_class_for_job_03() throws Exception {
		// search job class with jobName is empty
		jobName = "";
		assertTrue(jf.findJobClassForJob(jobName).length == 0);
		// search job class with jobName is null
		jobName = null;
		assertTrue(jf.findJobClassForJob(jobName).length == 0);
	}
}
	