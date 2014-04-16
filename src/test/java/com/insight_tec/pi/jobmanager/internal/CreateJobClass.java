package com.insight_tec.pi.jobmanager.internal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.PropertySettingJobFactory;
import org.wiperdog.jobmanager.JobClass;
import org.wiperdog.jobmanager.JobFacade;
import org.wiperdog.jobmanager.internal.JobFacadeImpl;


import static org.junit.Assert.*;

public class CreateJobClass {
	private SchedulerFactory sf;
	private Scheduler scheduler;
	JobFacade jf;
	String jobClassName;
	int concurrency;
	long maxruntime;
	long maxwaittime;
	
	@Before
	public void setup() throws Exception {
		sf = new StdSchedulerFactory();
		scheduler = sf.getScheduler();
		PropertySettingJobFactory jfactory = new PropertySettingJobFactory();
		jfactory.setWarnIfPropertyNotFound(false);
		scheduler.setJobFactory(jfactory);
		scheduler.start();
		jf = new JobFacadeImpl(scheduler);
		
		// Set parameters for assign func
		jobClassName = "class1";
		concurrency = 2;
		maxruntime = 400;
		maxwaittime = 500;
	}
	
	@After
	public void shutdown() throws Exception {
		scheduler.shutdown();
		scheduler = null;
		sf = null;
	}
	
	/**
	 * Create job class with jobClassName doesn't exist.
	 * Expected: create job class success.
	 */
	@Test
	public void create_job_class_01() throws Exception {
		// create job class with jobClassName doesn't exist
		JobClass jc = jf.createJobClass(jobClassName, concurrency, maxwaittime, maxruntime);
		assertEquals(jobClassName, jc.getName());
		assertEquals(concurrency, jc.getConcurrency());
		assertEquals(maxruntime, jc.getMaxRunTime());
		assertEquals(maxwaittime, jc.getMaxWaitTime());
	}

	/**
	 * Create job class with jobClassName already exist. 
	 * Expected: create job class success.
	 */
	@Test
	public void create_job_class_02() throws Exception {
		// create job class jc1 with jobClassName doesn't exit
		JobClass jc1 = jf.createJobClass(jobClassName, concurrency, maxwaittime, maxruntime);
		// create job class jc2 with jobClassName already exist
		JobClass jc2 = jf.createJobClass(jobClassName, concurrency, maxwaittime, maxruntime);
		assertEquals(jc1, jc2);
		assertEquals(jobClassName, jc2.getName());
		assertEquals(concurrency, jc2.getConcurrency());
		assertEquals(maxruntime, jc2.getMaxRunTime());
		assertEquals(maxwaittime, jc2.getMaxWaitTime());
	}

	/**
	 * Create job class with jobClassName is empty.
	 * Expected: create job class failure.
	 */
	@Test
	public void create_job_class_03() {
		jobClassName = "";
		JobClass jc = null;
		try{
			// create job class with jobClassName is empty
			jc = jf.createJobClass(jobClassName, concurrency, maxwaittime, maxruntime);
		} catch (Exception e) {
			assertNull(jc);
			assertTrue(e.getMessage().contains("JobListener name cannot be empty"));
			System.out.println(e);
		}
	}

	/**
	 * Create job class with jobClassName is null 
	 * Expected: create job class failure.
	 */
	@Test
	public void create_job_class_04() {
		jobClassName = null;
		JobClass jc = null;
		try{
			// create job class with jobClassName is null
			jc = jf.createJobClass(jobClassName, concurrency, maxwaittime, maxruntime);
		} catch (Exception e) {
			assertNull(jc);
			assertTrue(e.getMessage().contains("JobListener name cannot be empty"));
			System.out.println(e);
		}
	}
}