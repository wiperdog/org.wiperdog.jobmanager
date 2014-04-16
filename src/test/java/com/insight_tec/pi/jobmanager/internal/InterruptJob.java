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

public class InterruptJob {
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
	 * Interrupt running job. 
	 * Expected: Interrupt running job success.
	 */
	@Test
	public void interrupt_job_01() throws Exception {
		// interrupt job running
		String jcname = "class";
		int concurrency = 1;
		long maxruntime = 20000; 
		long maxwaittime = 20000;
		String jobName1 = "jobTest1";
		String jobName2 = "jobTest2";
		JobClass jc = jf.createJobClass(jcname, concurrency, maxruntime, maxwaittime);
		jf.assignJobClass(jobName1, jcname);
		jf.assignJobClass(jobName2, jcname);
		JobExecutable executable = new Common(path + "/" + jobName1, "class", "sender");
		JobDetail jd = jf.createJob(executable);
		Trigger tr1 = jf.createTrigger(jobName1, 0, 1);
		jf.scheduleJob(jd, tr1);
		Trigger tr2 = jf.createTrigger(jobName2, 0, 10);
		jf.scheduleJob(jd, tr2);
		boolean flag = false;
		// wait for job running
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
		// wait for interrupt job
		while(true) {
			if(jf.interruptJob(jobName1)) {
				System.out.println("interrupt job success!!!");
				break;
			}
		}
		assertFalse(jf.interruptJob(jobName2));
	}

	/**
	 * Interrupt with jobName is empty.
	 * Expected: can not interrupt jobName is empty.
	 */
	@Test
	public void interrupt_job_02() throws Exception {
		// interrupt with job no schedule
		String jobName = "jobTest";
		assertFalse(jf.interruptJob(jobName));
		// interrupt with jobName is empty
		jobName = "";
		assertFalse(jf.interruptJob(jobName));
	}
	
	/**
	 * Interrupt with jobName is null.
	 * Expected: can not interrupt jobName is null. 
	 */
	@Test
	public void interrupt_job_03() {
		try {
			// interrupt with jobName is null
			String jobName = null;
			jf.interruptJob(jobName);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("Name cannot be null"));
			System.out.println(e);
		}
	}
}
	