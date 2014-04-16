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

public class AssignJobClass {
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
	 * Assign job to job class already exists.
	 * Expected: job will be add to list assigned.
	 */
	@Test
	public void assign_job_class_01() throws Exception {
		// create job class with jobClassName doesn't exist
		JobClass jc = jf.createJobClass(jobClassName, concurrency, maxwaittime, maxruntime);
		// assign job class with jobClassName already exists
		jf.assignJobClass("testJob01",jobClassName);
		assertEquals("[DEFAULT.testJob01]", jc.getAssignedList().toString());
	}

	/**
	 * Assign job to job class does not exists.
	 * Expected: error occur "no such jobclass(className) exist".
	 */
	@Test
	public void assign_job_class_02() {
		try {
			// assign job class with jobClassName doesn't exist
			jf.assignJobClass("testJob01",jobClassName);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("no such jobclass(class1) exist"));
			System.out.println(e);
		}
	}

	/**
	 * Assign job class with jobName is empty.
	 * Expected: error occur "Name cannot be empty".
	 */
	@Test
	public void assign_job_class_03() {
		try{
			// create job class with jobClassName doesn't exist
			jf.createJobClass(jobClassName, concurrency, maxwaittime, maxruntime);
			// assign job class with jobName is empty
			jf.assignJobClass("", jobClassName);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("Name cannot be empty"));
			System.out.println(e);
		}
	}

	/**
	 * Assign job class with jobName is null.
	 * Expected: error occur "Name cannot be null".  
	 */
	@Test
	public void assign_job_class_04() {
		try{
			// create job class with jobClassName doesn't exist
			jf.createJobClass(jobClassName, concurrency, maxwaittime, maxruntime);
			// assign job class with jobName is null
			jf.assignJobClass(null, jobClassName);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("Name cannot be null"));
			System.out.println(e);
		}
	}

	/**
	 * Assign job class with jobClassName is empty.
	 * Expected: error occur "no such jobclass() exist". 
	 */
	@Test
	public void assign_job_class_05() {
		try{
			jobClassName = "";
			// assign job class with jobClassName is empty
			jf.assignJobClass("testJob01",jobClassName);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("no such jobclass() exist"));
			System.out.println(e);
		}
	}

	/**
	 * Assign job class with jobClassName is null.
	 * Expected: error occur "no such jobclass(null) exist". 
	 */
	@Test
	public void assign_job_class_06() {
		try {
			jobClassName = null;
			// assign job class with jobClassName is null
			jf.assignJobClass("testJob01",jobClassName);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("no such jobclass(null) exist"));
			System.out.println(e);
		}
	}
}
