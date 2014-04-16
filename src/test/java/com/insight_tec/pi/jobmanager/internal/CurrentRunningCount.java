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

public class CurrentRunningCount {
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
	 * Get current running count with one job is running.
	 * Prevent the concurrent executing job based on pre-configured number of concurrency.
	 * Expected: only one job running and CurrentRunningCount is 1.
	 */
	@Test
	public void current_running_count_01() throws Exception {
		// number job running will be corresponding to concurrency
		String jcname = "JobClass";
		int concurrency = 2;
		long maxruntime = 20000; 
		long maxwaittime = 20000;
		String jobName1 = "jobTest1";
		String jobName2 = "jobTest2";
		JobClass jc = jf.createJobClass(jcname, concurrency, maxruntime, maxwaittime);
		jf.assignJobClass(jobName1, jcname);
		jf.assignJobClass(jobName2, jcname);
		// create job detail for jobName1
		JobExecutable executable = new Common(path + "/" + jobName1, "class", "sender");
		JobDetail jd = jf.createJob(executable);
		// create trigger and schedule for jobName1
		Trigger tr1 = jf.createTrigger(jobName1, 0, 1);
		jf.scheduleJob(jd, tr1);
		// create trigger and schedule for jobName2
		Trigger tr2 = jf.createTrigger(jobName2, 0, 1);
		jf.scheduleJob(jd, tr2);
		boolean flag = false;
		while(true){
			System.out.println("Current Running Count: " + jc.getCurrentRunningCount());
			if(jc.getCurrentRunningCount() > 0) {
				System.out.println("Current Running Count: " + jc.getCurrentRunningCount());
				assertEquals(1, jc.getCurrentRunningCount());
				flag = true;
				break;
			}
		}
		assertTrue(flag);
	}
	
	/**
	 * Get current running count with two job is running.
	 * Prevent the concurrent executing job based on pre-configured number of concurrency.
	 * Expected: two job running and CurrentRunningCount is 2.
	 */
	@Test
	public void current_running_count_02() throws Exception {
		// number job running will be corresponding to concurrency
		String jcname = "JobClass";
		int concurrency = 2;
		long maxruntime = 20000; 
		long maxwaittime = 20000;
		String jobName1 = "jobTest1";
		String jobName2 = "jobTest2";
		JobClass jc = jf.createJobClass(jcname, concurrency, maxruntime, maxwaittime);
		jf.assignJobClass(jobName1, jcname);
		jf.assignJobClass(jobName2, jcname);
		// create job detail for jobName1
		JobExecutable executable = new Common(path + "/" + jobName1, "class", "sender");
		JobDetail jd = jf.createJob(executable);
		// create trigger and schedule for jobName1
		Trigger tr1 = jf.createTrigger(jobName1, 0, 1);
		jf.scheduleJob(jd, tr1);
		// create job detail for jobName2
		JobExecutable executable2 = new Common(path + "/" + jobName2, "class", "sender");
		JobDetail jd2 = jf.createJob(executable2);
		// create trigger and schedule for jobName2
		Trigger tr2 = jf.createTrigger(jobName2, 0, 1);
		jf.scheduleJob(jd2, tr2);
		boolean flag = false;
		while(true){
			System.out.println("Current Running Count: " + jc.getCurrentRunningCount());
			if(jc.getCurrentRunningCount() > 1) {
				System.out.println("Current Running Count: " + jc.getCurrentRunningCount());
				assertEquals(2, jc.getCurrentRunningCount());
				flag = true;
				break;
			}
		}
		assertTrue(flag);
	}
}
	