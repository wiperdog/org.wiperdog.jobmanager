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

public class RevokeJob {
	private SchedulerFactory sf;
	private Scheduler scheduler;
	JobFacade jf;
	String path = System.getProperty("user.dir");
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
		
		// set value for parameters of assign func
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
	 * Revoke job with jobClassName already exists. 
	 * Expected: the assigned list will be revoke the job corresponding.
	 */
	@Test
	public void revoke_job_01() throws Exception {
		// create job class with jobClassName doesn't exist
		JobClass jc = jf.createJobClass(jobClassName, concurrency, maxwaittime, maxruntime);
		// assign job class with jobClassName already exists
		jf.assignJobClass("testJob01",jobClassName);
		jf.assignJobClass("testJob02",jobClassName);
		assertEquals("[DEFAULT.testJob01, DEFAULT.testJob02]", jc.getAssignedList().toString());
		// revoke job with jobClassName already exists
		jf.revokeJobClass("testJob02", jobClassName);
		assertEquals("[DEFAULT.testJob01]", jc.getAssignedList().toString());
	}

	/**
	 * Revoke job with jobClassName doesn't exists.
	 * Expected: revoke job failure and error occur "no such jobclass(className) exist".
	 */
	@Test
	public void revoke_job_02() {
		try {
			jobClassName = "ABC";
			// revoke job with jobClassName doesn't exists
			jf.revokeJobClass("testJob02", jobClassName);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("no such jobclass(ABC) exist"));
			System.out.println(e);
		}
	}
	
	/**
	 * Revoke job with jobName does not exist. 
	 * Expected: the assigned list will not change.
	 */
	@Test
	public void revoke_job_03() throws Exception {
		// create job class with jobClassName doesn't exist
		JobClass jc = jf.createJobClass(jobClassName, concurrency, maxwaittime, maxruntime);
		// assign job class with jobClassName already exists
		jf.assignJobClass("testJob01",jobClassName);
		jf.assignJobClass("testJob02",jobClassName);
		assertEquals("[DEFAULT.testJob01, DEFAULT.testJob02]", jc.getAssignedList().toString());
		// revoke job with jobName does not exist
		jf.revokeJobClass("testJob03", jobClassName);
		assertEquals("[DEFAULT.testJob01, DEFAULT.testJob02]", jc.getAssignedList().toString());
	}
	
	/**
	 * Revoke job with jobName is null.
	 * Expected: revoke job failure and error occur "Name cannot be null".
	 */
	@Test
	public void revoke_job_04() {
		try {
			// create job class with jobClassName doesn't exist
			jf.createJobClass(jobClassName, concurrency, maxwaittime, maxruntime);
			// revoke job with jobName is null and jobClassName already exists
			jf.revokeJobClass(null, jobClassName);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("Name cannot be null"));
			System.out.println(e);
		}
	}
	
	/**
	 * Revoke job with jobClassName is empty.
	 * Expected: revoke job failure and error occur "no such jobclass() exist".
	 */
	@Test
	public void revoke_job_05() {
		try {
			jobClassName = "";
			// revoke job with jobClassName is empty
			jf.revokeJobClass("testJob02", jobClassName);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("no such jobclass() exist"));
			System.out.println(e);
		}
	}
	
	/**
	 * Revoke job with jobClassName is null.
	 * Expected: revoke job failure and error occur "no such jobclass(null) exist".
	 */
	@Test
	public void revoke_job_06() {
		try {
			jobClassName = null;
			// revoke job with jobClassName is null
			jf.revokeJobClass("testJob02", jobClassName);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("no such jobclass(null) exist"));
			System.out.println(e);
		}
	}
}
	