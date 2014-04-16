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

public class UnscheduleJob {
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
	 * Unschedule with the trigger already exist 
	 * Expected: the trigger corresponding to the job will be remove.
	 */
	@Test
	public void search_job_class_for_job_01() throws Exception {
		jobName = "jobTest";
		JobExecutable executable = new Common(path + "/" + jobName, "class", "sender");
		JobDetail jd = jf.createJob(executable);
		Trigger tr = jf.createTrigger(jobName, 0, 200);
		// process schedule for job
		jf.scheduleJob(jd, tr);
		assertEquals("DEFAULT.jobTest", jf.getTrigger(jobName).getKey().toString());
		// unschedule for trigger already exist
		jf.unscheduleJob(tr);
		assertNull(jf.getTrigger(jobName));
	}
	
	/**
	 * Unschedule with the trigger does not exist.
	 * The trigger has not been create scheule.
	 * Expected: Can not unschedule with the trigger does not exist into schedule.
	 */
	@Test
	public void search_job_class_for_job_02() {
		try {
			Trigger tr = jf.createTrigger(jobName, 0, 200);
			// unschedule for trigger does not exist
			jf.unscheduleJob(tr);
		} catch (Exception e) {
			assertTrue(e.toString().contains("java.lang.NullPointerException"));
			System.out.println(e);
		}		
	}
	
	/**
	 * Unschedule with the trigger is empty. 
	 * Expected: Can not unschedule with null
	 */
	@Test
	public void search_job_class_for_job_03() {
		try {
			Trigger tr = null;
			// unschedule for trigger does not exist
			jf.unscheduleJob(tr);
		} catch (Exception e) {
			assertTrue(e.toString().contains("java.lang.NullPointerException"));
			System.out.println(e);
		}	
	}
}	