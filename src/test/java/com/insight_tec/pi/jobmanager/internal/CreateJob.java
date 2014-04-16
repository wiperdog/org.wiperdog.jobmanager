package com.insight_tec.pi.jobmanager.internal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.PropertySettingJobFactory;
import org.wiperdog.jobmanager.JobExecutable;
import org.wiperdog.jobmanager.JobFacade;
import org.wiperdog.jobmanager.internal.JobFacadeImpl;

import static org.junit.Assert.*;

public class CreateJob {
	private SchedulerFactory sf;
	private Scheduler scheduler;
	JobFacade jf;
	String path = System.getProperty("user.dir");
	
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
	 * Create job with jobName is not empty.
	 * Expected: create job success.
	 */
	@Test
	public void create_job_01() throws Exception {
		String jobName = "/jobTest.job";
		// create job with jobName is "jobTest.job"
		JobExecutable executable = new Common(path + jobName, "class", "sender");
		JobDetail jd = jf.createJob(executable);
		assertNotNull(jd);
		assertEquals("DEFAULT.jobTest.job", jd.getKey().toString());
		assertEquals("class org.wiperdog.jobmanager.internal.ObjectJob", jd.getJobClass().toString());
	}

	/**
	 * Create job with jobName is empty.
	 * Expected: create job failure. Error occur "Job name cannot be empty".
	 */
	@Test
	public void create_job_02() {
		try {
			String jobName = "/ ";
			// create job with jobName is empty
			JobExecutable executable = new Common(path + jobName, "class", "sender");
			jf.createJob(executable);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("Job name cannot be empty"));
			System.out.println(e);
		}
	}
}
