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
import org.wiperdog.jobmanager.JobClass;
import org.wiperdog.jobmanager.JobExecutable;
import org.wiperdog.jobmanager.JobFacade;
import org.wiperdog.jobmanager.internal.JobFacadeImpl;

import static org.junit.Assert.*;

public class RemoveJob {
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
	 * remove the jobdetail already exist. 
	 * Expected: remove the jobdetail success. 
	 */
	@Test
	public void current_running_count_01() throws Exception {
		String path = System.getProperty("user.dir");
		String jobName = "jobTest";
		// create job with jobName is "jobTest.job"
		JobExecutable executable = new Common(path + "/" + jobName, "class", "sender");
		JobDetail jd = jf.createJob(executable);
		assertEquals("DEFAULT.jobTest.job", jd.getKey().toString());
		// remove jobdetail
		boolean flag = jf.removeJob(jd);
		assertTrue(flag);
	}
}
