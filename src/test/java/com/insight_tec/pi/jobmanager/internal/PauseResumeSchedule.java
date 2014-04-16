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

public class PauseResumeSchedule {
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
	 * pause and resume the schedule. 
	 * Expected: pause and resume the schedule success.
	 */
	@Test
	public void pause_resume_schedule_01() throws Exception {
		// check pause and resume schedule
		String jcname = "class";
		int concurrency = 1;
		long maxruntime = 20000; 
		long maxwaittime = 20000;
		String jobName1 = "jobTest1";
		JobClass jc = jf.createJobClass(jcname, concurrency, maxruntime, maxwaittime);
		jf.assignJobClass(jobName1, jcname);
		JobExecutable executable = new Common(path + "/" + jobName1, "class", "sender");
		JobDetail jd = jf.createJob(executable);
		Trigger tr1 = jf.createTrigger(jobName1, 0, 1);
		jf.scheduleJob(jd, tr1);
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
		while(true) {
			// pause schedule
			jf.pause();
			Thread.sleep(500);
			assertEquals(0, jc.getCurrentRunningCount());
			// resume schedule
			jf.resume();
			Thread.sleep(500);
			assertEquals(1, jc.getCurrentRunningCount());
			break;
		}
	}
}
	