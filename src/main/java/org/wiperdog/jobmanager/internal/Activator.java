package org.wiperdog.jobmanager.internal;

import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.PropertySettingJobFactory;
import org.wiperdog.jobmanager.JobFacade;
import org.wiperdog.jobmanager.JobManagerException;
import org.wiperdog.rshell.api.RShellProvider;

public class Activator implements BundleActivator {
	public static final String PID = "org.wiperdog.jobmanager";
	public static final String LOGGERNAME = PID;
	public static final String KEY_MAXRECEIVESIZE = "shell.maxreceivesize";
	public static final String KEY_MAXHISTORYDEPTH = "maxhistorydepth";
	private BundleContext context;

	private Scheduler scheduler;
	private SchedulerFactory sf;

	private JobFacade jf;
	
	private Logger logger = Logger.getLogger(Activator.class);

	/**
	 * CommanderJobにはCommanderServiceが必要
	 * @author kurohara
	 *
	 */
	private class CommanderServiceTrackerCustomizer implements ServiceTrackerCustomizer {

		public Object addingService(ServiceReference reference) {
			Object svc = context.getService(reference);
			return svc;
		}
		public void modifiedService(ServiceReference reference, Object service) {
		}
		public void removedService(ServiceReference reference, Object service) {
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void start(BundleContext context) throws Exception {
		this.context = context;
		initSchedulerCore();
		
		context.registerService(JobFacade.class.getName(), jf, null);
		
		Dictionary props = new Hashtable();
		props.put("osgi.command.scope", "scheduler");
		props.put("osgi.command.function", new String [] {"load", "list"});
		
		ServiceTracker tracker = new ServiceTracker(context, RShellProvider.class.getName(), new CommanderServiceTrackerCustomizer());
		tracker.open();
	}
	
	public void stop(BundleContext context) throws Exception {
		finishSchedulerCore();
	}

	
	
	private void initSchedulerCore() {
		sf = new StdSchedulerFactory();
		try {
			scheduler = sf.getScheduler();
			PropertySettingJobFactory jfactory = new PropertySettingJobFactory();
			jfactory.setWarnIfPropertyNotFound(false);
			scheduler.setJobFactory(jfactory);
			jf = new JobFacadeImpl(scheduler);
			scheduler.start();
		} catch (SchedulerException e) {
			logger.trace("", e);
		} catch (JobManagerException e) {
			logger.trace("", e);
		}
	}
	
	private void finishSchedulerCore() {
		try {
			jf = null;
			scheduler.shutdown();
			scheduler = null;
			sf = null;
		} catch (SchedulerException e) {
			logger.trace("", e);
		}
	}

}