package com.insight_tec.pi.jobmanager.internal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.PropertySettingJobFactory;
import org.wiperdog.jobmanager.JobExecutable;
import org.wiperdog.jobmanager.JobFacade;
import org.wiperdog.jobmanager.internal.JobFacadeImpl;

import static org.junit.Assert.*;

public class ScheduleAndRescheduleJob {
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
		jobName = "jobTest1";
	}
	
	@After
	public void shutdown() throws Exception {
		scheduler.shutdown();
		scheduler = null;
		sf = null;
	}
	
	/**
	 * Create schedule for job with jobDetail and trigger.
	 * The schedule does not exist. 
	 * Expected: create schedule success.
	 */
	@Test
	public void schedule_and_reschedule_job_01() throws Exception {
		JobExecutable executable = new Common(path + "/" + jobName, "class", "sender");
		JobDetail jd = jf.createJob(executable);
		Trigger tr = jf.createTrigger(jobName, 0, 200);
		// process schedule for job
		jf.scheduleJob(jd, tr);
		System.out.println("====1: " + jf.getTrigger(jobName));
	}

	/**
	 * Create schedule for job with jobDetail and trigger.
	 * The schedule already exist. 
	 * Expected: create schedule success. 
	 */
	@Test
	public void schedule_and_reschedule_job_02() throws Exception {
		JobExecutable executable = new Common(path + "/" + jobName, "class", "sender");
		JobDetail jd = jf.createJob(executable);
		Trigger tr = jf.createTrigger(jobName, 0, 200);
		// process schedule for job
		jf.scheduleJob(jd, tr);
		Thread.sleep(5000);
		// re-schedule for job
		tr = jf.createTrigger(jobName, 10, 100);
		jf.scheduleJob(jd, tr);
	}
	
	/**
	 * Create schedule for job with jobDetail is null. 
	 * Expected: create schedule failure.
	 */
	@Test
	public void schedule_and_reschedule_job_03() throws Exception {
		try {
			JobDetail jd = null;
			Trigger tr = jf.createTrigger(jobName, 0, 200);
			// process schedule for job with jobdetail is null
			jf.scheduleJob(jd, tr);
		} catch (Exception e) {
			assertTrue(e.toString().contains("java.lang.NullPointerException")); 
			System.out.println(e);
		}
	}
	
	/**
	 * Create schedule for job with Trigger is null. 
	 * Expected: create schedule failure.
	 */
	@Test
	public void schedule_and_reschedule_job_04() {
		try {
			JobExecutable executable = new Common(path + "/" + jobName, "class", "sender");
			JobDetail jd = jf.createJob(executable);
			Trigger tr = null;
			// process schedule for job with Trigger is null
			jf.scheduleJob(jd, tr);
		} catch (Exception e) {
			assertTrue(e.toString().contains("java.lang.NullPointerException")); 
			System.out.println(e);
		}
	}
}
	