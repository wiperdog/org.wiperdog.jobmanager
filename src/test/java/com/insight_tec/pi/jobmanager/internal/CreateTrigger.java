package com.insight_tec.pi.jobmanager.internal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.PropertySettingJobFactory;
import org.wiperdog.jobmanager.JobFacade;
import org.wiperdog.jobmanager.internal.JobFacadeImpl;

import static org.junit.Assert.*;

public class CreateTrigger {
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
	 * Create trigger with the parameters: jobName, delay, interval
	 * Expected: create trigger success.
	 */
	@Test
	public void create_trigger_01() throws Exception {
		String jobName = "jobTest";
		// create Trigger with jobName, delay, interval
		Trigger tr = jf.createTrigger(jobName, 0, 200);
		assertNotNull(tr);
		assertEquals("DEFAULT.jobTest", tr.getKey().toString());
	}

	/**
	 * Create trigger with value of jobName is empty.
	 * Expected: create trigger failure.
	 */
	@Test
	public void create_trigger_02() {
		try {
			String jobName = "";
			// create Trigger with jobName, delay, interval
			// value of jobName is empty
			jf.createTrigger(jobName, 0, 200);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("Trigger name cannot be null or empty"));
			System.out.println(e);
		}
	}
	
	/**
	 * Create trigger with value of jobName is null. 
	 * Expected: create trigger failure.
	 */
	@Test
	public void create_trigger_03() {
		try {
			String jobName = null;
			// create Trigger with jobName, delay, interval
			// value of jobName is null
			jf.createTrigger(jobName, 0, 200);
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("Trigger name cannot be null or empty"));
			System.out.println(e);
		}
	}
}