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

public class DeleteJobClass {
	private SchedulerFactory sf;
	private Scheduler scheduler;
	JobFacade jf;
	
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
	 * Delete job class already exists.
	 * Expected: delete job class success. 
	 */
	@Test
	public void delete_job_class_01() {
		try {
			String jcname = "class";
			int concurrency = 2;
			long maxruntime = 400;
			long maxwaittime = 500;
			// create job class with jobClassName doesn't exist
			JobClass jc = jf.createJobClass(jcname, concurrency, maxwaittime, maxruntime);
			jf.assignJobClass("test01", jcname);
			assertEquals("[DEFAULT.test01]", jc.getAssignedList().toString());
			// delete job class class already exists
			jf.deleteJobClass(jcname);
			jf.assignJobClass("test02", jcname);
		} catch (Exception e) {
			System.out.println(e);
			assertTrue(e.getMessage().contains("no such jobclass(class)"));
			System.out.println(e);
		}
	}

	/**
	 * Delete job class does not exists. 
	 * Expected: delete job class failure.
	 */
	@Test
	public void delete_job_class_02() {
		try {
			// delete job class does not exist
			jf.deleteJobClass("classTest");
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("no such jobclass(classTest)"));
			System.out.println(e);
		}
	}
	
	/**
	 * Delete job class with job class name is empty. 
	 * Expected: delete job class failure.
	 */
	@Test
	public void delete_job_class_03() {
		try {
			String jcname = "";
			// delete job class with job class name is empty
			jf.deleteJobClass(jcname);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("no such jobclass()"));
			System.out.println(e);
		}
	}
	
	/**
	 * Delete job class with job class name is null 
	 * Expected: delete job class failure.
	 */
	@Test
	public void delete_job_class_04() {
		try {
			String jcname = null;
			// delete job class with job class name is null
			jf.deleteJobClass(jcname);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("no such jobclass(null)"));
			System.out.println(e);
		}
	}
}
	