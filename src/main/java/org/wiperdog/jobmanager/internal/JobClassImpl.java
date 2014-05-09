package org.wiperdog.jobmanager.internal;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.quartz.DateBuilder;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.ListenerManager;
import org.quartz.Matcher;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.UnableToInterruptJobException;
import org.quartz.impl.matchers.KeyMatcher;
import org.quartz.impl.StdSchedulerFactory;

import org.wiperdog.jobmanager.Constants;
import org.wiperdog.jobmanager.JobClass;
import org.wiperdog.jobmanager.JobFacade;

public class JobClassImpl implements JobClass {

	private String name;
	private int numConcurrency;
	private long maxRunTime;
	private long maxWaitTime;
	private List<JobKey> assignedList = new ArrayList<JobKey>();
		
	private Set<JobKey> running = new HashSet<JobKey>();
	private BlockingQueue<VetoedTriggerKey> vetoedQueue = new LinkedBlockingQueue<VetoedTriggerKey>();
	private final static String REASONKEY_CONCURRENCY = JobClassImpl.class.getName();	
	public static final String SUFFIX_CANCELJOB = "_cancel";
	
	private final static String NULLNAME = "NULLNAME";
	private final static String NULLGROUP = "NULLGROUP";
	private static final Matcher<JobKey> NULLJOBMATCHER = KeyMatcher.keyEquals(new JobKey(NULLNAME, NULLGROUP));
	
	private final Scheduler scheduler;
	private static Scheduler cancelJobScheduler;
	
	private Logger logger = Logger.getLogger(JobClassImpl.class);

	public JobClassImpl(Scheduler sched, String name) throws SchedulerException {		
		this.name = name;
		this.scheduler = sched;
		// default numConcurrency is 1
		this.numConcurrency = 1;
		// default runtime = near forever
		this.maxRunTime = Long.MAX_VALUE;
		// default wait time = near forever
		this.maxWaitTime = Long.MAX_VALUE;
		ListenerManager lm = sched.getListenerManager();
		lm.addJobListener(new JobClassJobListener(), NULLJOBMATCHER);
		
		//Init cancel scheduler
		if (cancelJobScheduler == null) {
			cancelJobScheduler = startCancelScheduler();
		}
		// Start cancel scheduler
		if (!cancelJobScheduler.isStarted() ) {
			cancelJobScheduler.start();
		}
	}
	
	public JobClassImpl(JobClassImpl src) throws SchedulerException {
		name = src.name;
		scheduler = src.scheduler;
		src.vetoedQueue.drainTo(vetoedQueue);
		running.addAll(src.running);
		numConcurrency = src.numConcurrency;
		maxWaitTime = src.maxWaitTime;
		maxRunTime = src.maxRunTime;
		assignedList.addAll(src.assignedList);
		ListenerManager lm = scheduler.getListenerManager();
		lm.removeJobListener(name);
		lm.addJobListener(new JobClassJobListener(), NULLJOBMATCHER);
		
		//Init cancel scheduler
		if (cancelJobScheduler == null) {
			cancelJobScheduler = startCancelScheduler();
		}
		// Start cancel scheduler
		if (!cancelJobScheduler.isStarted() ) {
			cancelJobScheduler.start();
		}
	}
	
	private static Scheduler startCancelScheduler()  throws SchedulerException{
		StdSchedulerFactory stdScheFac = new StdSchedulerFactory();
		Properties props = new Properties();
 		props.setProperty(StdSchedulerFactory.PROP_THREAD_POOL_CLASS,
 				"org.quartz.simpl.SimpleThreadPool");
 		props.put("org.quartz.threadPool.threadCount", "20");
 		props.put("org.quartz.scheduler.instanceId","CancelJobScheduler");
 		props.put("org.quartz.scheduler.instanceName","CancelJobScheduler");
 		props.put("org.quartz.scheduler.skipUpdateCheck","true");
 	    props.put("org.quartz.threadPool.threadPriority","5");
 		props.put("org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread","true");
 		stdScheFac.initialize(props);
 		return stdScheFac.getScheduler();
	}
	
	public void close() {
		ListenerManager lm;
		try {
			lm = scheduler.getListenerManager();
			lm.removeJobListener(name);
		} catch (SchedulerException e) {			
			logger.error("failed to close JobClass: " + name, e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobClass#getJobClassName()
	 */
	public String getName() {
		return this.name;
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobClass#getConcurrency()
	 */
	public int getConcurrency() {
		return this.numConcurrency;
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobClass#setConcurrency(int)
	 */
	public void setConcurrency(int concurrency) {
		this.numConcurrency = concurrency;
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobClass#getMaxRunTime()
	 */
	public long getMaxRunTime() {
		return this.maxRunTime;
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobClass#setMaxRunTime(long)
	 */
	public void setMaxRunTime(long maxRunTime) {
		this.maxRunTime = maxRunTime;
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobClass#getMaxWaitTime()
	 */
	public long getMaxWaitTime() {
		return this.maxWaitTime;
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobClass#setMaxWaitTime(long)
	 */
	public void setMaxWaitTime(long maxWaitTime) {
		this.maxWaitTime = maxWaitTime;
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobClass#getAssignedList()
	 */
	public List<JobKey> getAssignedList() {
		return assignedList;
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobClass#getCurrentRunningCount()
	 */
	public int getCurrentRunningCount() {
		return running.size();
	}
	
	/**
	 * Add it to the Job class.
	 * @param key Key job
	 */
	public void addJob(JobKey key) {
		logger.info("Add Job: " + key.toString());
		try {
			assignedList.add(key);
			scheduler.getListenerManager().addJobListenerMatcher(name, KeyMatcher.keyEquals((key)));
		} catch (SchedulerException e) {
			logger.info("Failed to add job: " + key.toString());
			logger.error(e.getMessage());
		}
	}

	/**
	 * Delete a job from the job class.
	 * @param key Key job
	 */
	public void deleteJob(JobKey key) {
		logger.info("Delete Job: " + key.toString());
		try {
			scheduler.getListenerManager().removeJobListenerMatcher(name, KeyMatcher.keyEquals(key));
			assignedList.remove(key);
		} catch (SchedulerException e) {
			logger.info("Failed to delete job: " + key.toString());
			logger.error("", e);
		}
	}

	/**
	 * Interrupt a job when executed
	 */
	public static final class RuntimeLimitterJob implements InterruptableJob {
		private Logger logger = Logger.getLogger(Activator.LOGGERNAME);
		public static final String KEY_JOBKEY = "jobkey";
		public static final String JOB_SCHEDULER = "jobscheduler";
		
		public RuntimeLimitterJob() {
			logger.trace("RuntimeLimitterJob.RuntimeLimitterJob()");
		}
		
		public void execute(JobExecutionContext context)
				throws JobExecutionException {
			logger.trace("RuntimeLimitterJob.execute()");
			JobKey jobkey = (JobKey) context.getMergedJobDataMap().get(KEY_JOBKEY);
			logger.debug("interrupting job(" + jobkey.getName() + ")");
			Scheduler jobScheduler = (Scheduler) context.getMergedJobDataMap().get(JOB_SCHEDULER);
			try {
				jobScheduler.interrupt(jobkey);
			} catch (UnableToInterruptJobException e) {
				logger.debug("	error on interrupt:", e);
			}
		}

		public void interrupt() throws UnableToInterruptJobException {
			logger.trace("RuntimeLimitterJob.interrupt()");
		}
	}

	/**
	 * @author nguyenvannghia
	 *
	 */
	public class JobClassJobListener implements JobListener {
		
		public JobClassJobListener() {			
		}

		/* (non-Javadoc)
		 * @see org.quartz.JobListener#jobToBeExecuted(org.quartz.JobExecutionContext)
		 */
		public void jobToBeExecuted(JobExecutionContext context) {
			Trigger trigger = context.getTrigger();
			JobKey key = trigger.getJobKey();
			JobDetail detail = context.getJobDetail();
			JobDataMap datamap = detail.getJobDataMap();
			logger.info(" Job with name:  " + key.getName() +" will be executed soon!");
			
			@SuppressWarnings("unchecked")
			Set<String> reasons = (Set<String>) datamap.get(Constants.KEY_PROHIBIT);
			if (reasons == null) {
				reasons = new HashSet<String>();
				datamap.put(Constants.KEY_PROHIBIT, reasons);
			}
			// trigger with bigger priority is "re-scheduled" for immediate execution
			if (running.size() >= numConcurrency || (! vetoedQueue.isEmpty() && trigger.getPriority() <= Trigger.DEFAULT_PRIORITY)) {
				VetoedTriggerKey vk = new VetoedTriggerKey(trigger.getKey(), detail);
				logger.info("Offering into vetoedQueue, size:" + vetoedQueue.size());
				vetoedQueue.offer(vk);
				reasons.add(REASONKEY_CONCURRENCY);
				logger.info("JobListener: numConcurrency exceeded for job: " + trigger.getJobKey().toString());
				return;
			}
			
			//
			// save key as running job
			running.add(key);
			// clear prohibit reason of concurrency check
			reasons.remove(REASONKEY_CONCURRENCY);
			if (maxRunTime != Long.MAX_VALUE && maxRunTime > 0) {
				logger.info("insert 'RUNTIMEOVER kill' job");
				// schedule runtime over cancelling job here.
				JobDetail cancelJob = newJob(RuntimeLimitterJob.class)
					    .withIdentity(key.getName() + SUFFIX_CANCELJOB, key.getGroup() + SUFFIX_CANCELJOB)
					    .build();
				cancelJob.getJobDataMap().put(RuntimeLimitterJob.KEY_JOBKEY, key);
				cancelJob.getJobDataMap().put(RuntimeLimitterJob.JOB_SCHEDULER, context.getScheduler());
				Trigger cancelTrigger = newTrigger()
					    .withIdentity(key.getName() + SUFFIX_CANCELJOB, key.getGroup() + SUFFIX_CANCELJOB)
					    .startAt( DateBuilder.futureDate((int) maxRunTime, IntervalUnit.MILLISECOND) )
					    .forJob(cancelJob)
					    .build();
				try {
					cancelJobScheduler.scheduleJob(cancelJob, cancelTrigger);
				} catch (SchedulerException e) {
					logger.info("failed to insert 'RUNTIMEOVER kill' job");
				}
			}
		}

		/* (non-Javadoc)
		 * @see org.quartz.JobListener#jobExecutionVetoed(org.quartz.JobExecutionContext)
		 */
		public void jobExecutionVetoed(org.quartz.JobExecutionContext parameter) {
			logger.info("ConcurrencyJobListener.jobExecutionVetoed()");
		}
		
		/* (non-Javadoc)
		 * @see org.quartz.JobListener#jobWasExecuted(org.quartz.JobExecutionContext, org.quartz.JobExecutionException)
		 */
		public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
			logger.info("ConcurrencyJobListener.jobWasExecuted()");
			Scheduler sched = context.getScheduler();
			try {
				sched.pauseAll();
			} catch (SchedulerException e1) {
				logger.error("Scheduler pause all action failed: " + e1.getMessage());
				logger.info("", e1);
			}

			try {
				// Delete the RuntimeLimitter job which was scheduled in method jobTobeExecuted()
				cancelJobScheduler.deleteJob(jobKey(context.getJobDetail().getKey().getName() + SUFFIX_CANCELJOB, context.getJobDetail().getKey().getGroup() + SUFFIX_CANCELJOB));
			} catch (SchedulerException e) {
				logger.error("Failed to delete RuntimeLimitter job for ("+context.getJobDetail().getKey().getName() + "), may already removed. Detail as: " 
							+ e.getMessage());
			}
			try {
				//Remove jobKey exist in running set
				if (running.remove(context.getTrigger().getJobKey())) {					
					/** 
					 * Remove oldest element from vetoed queue, then check if it can be re-scheduled
					 *  based on its maxWaitTime.If it can not be, throw it away. 					 
					 */
					while (true) {
						VetoedTriggerKey vetoedkey = vetoedQueue.poll();
						if (vetoedkey != null) {
							try {
								JobDetail job = vetoedkey.getJobDetail();
								// Re-schedule with "run immediately trigger" if it can proceed.
								if (vetoedkey.canProceed(maxWaitTime)) {
									logger.info("Re-launching the waiting job: " + vetoedkey.getJobDetail().getKey().toString()+ ":" + vetoedkey.getKey().toString());
									// Check if job was durable
									if (sched.checkExists(job.getKey())) {
										Trigger oldTrigger = sched.getTrigger(vetoedkey.getKey());
										Trigger rft = newTrigger()
												.startNow()
												// to distinguish re-fire trigger, set priority to DEFAULT + 1
												.withPriority(Trigger.DEFAULT_PRIORITY + 1)
												.forJob(job)
												.build();
										if (oldTrigger != null && ! oldTrigger.mayFireAgain()) {
											sched.rescheduleJob(vetoedkey.getKey(), rft);
											logger.info("Re-schedule job: " + vetoedkey.getKey().toString() + ":" + rft.getKey().toString());
										}  else {
											sched.scheduleJob(rft);
											logger.info("Schedule Job: " + job.getKey().toString() + ":" + rft.getKey().toString());
										}
									} else {
										Trigger rft = newTrigger()
												.startNow()
												// to distinguish re-fire trigger, set priority to DEFAULT + 1
												.withPriority(Trigger.DEFAULT_PRIORITY + 1)
												.forJob(job.getKey())
												.build();
										sched.scheduleJob(job, rft);
										logger.info("Schedule Job " + job.getKey().toString() + ":" + rft.getKey().toString());
									}
									if (logger.isTraceEnabled()) {
										Trigger rft = sched.getTrigger(vetoedkey.getKey());
										logger.info(
												"Trigger is updated for re-launch: " + vetoedkey.getKey().toString() + 
												" { nextfiretime : " + rft.getNextFireTime() + 
												", previousfiretime : " + rft.getPreviousFireTime() + "}");
									}
									break;
								} else {
									expiredWaitTime(vetoedkey);
								}
							} catch (SchedulerException e) {
								logger.info("Failed to re-schedule vetoed job: " + vetoedkey.toString());
								logger.error("", e);
							} catch (Throwable t) {
								logger.info("Something has been thrown at post processing of job execution.", t);
							}
						} else {
							break;
						}
					}
				}
			} catch (Throwable t) {
				logger.info("Something has been thrown at post processing of job execution.", t);
			} finally {
				try {
					sched.resumeAll();
				} catch (SchedulerException e) {
					logger.info("failed to resume scheduler");
					logger.error("", e);
				}
				logger.error("schduler resumed");
			}
		}
		
		/* (non-Javadoc)
		 * @see org.quartz.JobListener#getName()
		 */
		public String getName() {
			return name;
		}

	}
	private void expiredWaitTime(VetoedTriggerKey vetoedkey) {
		// wait time is over, giving up, throw it away.
		logger.info("waitTime exceeded for job:" + vetoedkey.getKey().toString() + ", giving up");			
	}
	public class VetoedTriggerKey {

		private Date vetoedAt;
		private TriggerKey triggerKey;
		private JobDetail jobDetail;
		private Logger logger = Logger.getLogger(VetoedTriggerKey.class);
		
		public VetoedTriggerKey(TriggerKey key, JobDetail job) {
			logger.trace("VetoedTriggerKey.VetoedTriggerKey(" + key.toString() + ")");
			this.triggerKey = key;
			this.jobDetail =  job;
			this.vetoedAt = new Date();
		}

		public VetoedTriggerKey(VetoedTriggerKey src) {
			vetoedAt = src.vetoedAt;
			triggerKey = src.triggerKey;
			jobDetail = src.jobDetail;
		}
		
		public TriggerKey getKey() {
			return triggerKey;
		}

		/**
		 * Test if a Trigger can be process based on MAX_WAIT_TIME
		 * @param waitTimeMs time for a job to be waited to be execute
		 */
		public boolean canProceed(long waitTimeMs) {
			if ((new Date()).getTime() - vetoedAt.getTime() < waitTimeMs) {
				logger.trace("VetoedTriggerKey.canProceed(" + waitTimeMs + ") - true");
				return true;
			}
			logger.trace("VetoedTriggerKey.canProceed(" + waitTimeMs + ") - false");
			return false;
		}

		public JobDetail getJobDetail() {
			return this.jobDetail;
		}
		public Date getVetoedDate() {
			return vetoedAt;
		}
		
		public String toString() {
			return "{ vetoed: '" + vetoedAt + "', trigger: '" + triggerKey.toString() + "', job: '" + jobDetail.getKey().toString() + "' }";
		}
	}

}