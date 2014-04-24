package org.wiperdog.jobmanager.internal;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.quartz.DateBuilder;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.TriggerListener;
import org.quartz.UnableToInterruptJobException;
import org.quartz.impl.matchers.EverythingMatcher;
import org.wiperdog.jobmanager.Constants;
import org.wiperdog.jobmanager.JobClass;
import org.wiperdog.jobmanager.JobExecutable;
import org.wiperdog.jobmanager.JobFacade;
import org.wiperdog.jobmanager.JobManagerException;

/**
 * @author nguyenvannghia
 *
 */
public class JobFacadeImpl implements JobFacade {
	
	private final Scheduler sched;
	
	private Logger logger = Logger.getLogger(JobFacadeImpl.class);
	
	private static long seqnum = 0;
	private static final String AUTONAME_BASE = "JOBOBJECT_";

	private String autoname() {
		return AUTONAME_BASE + ++seqnum;
	}

	private String autoname(String name) {
		if (name == null || Constants.NAME_AUTONAME.equals(name)) {
			return autoname();
		}
		return name;
	}
	/**
	 * Map contains created job classes
	 */
	private Map<String,JobClassImpl> jobClassMap = new HashMap<String,JobClassImpl>();
	
	/**
	 * Map contains created jobs 
	 */
	private Map<String, JobDetail> jobDetailMap = new HashMap<String,JobDetail>();

	public JobFacadeImpl(Scheduler sched) throws JobManagerException {
		logger.trace("constructor JobFacadeImpl(" + sched.toString() + ")");
		this.sched = sched;
		// add trigger listener
		try {
			sched.getListenerManager().addTriggerListener(new FacadeTriggerListener("JobFacadeTriggerListener"), EverythingMatcher.allTriggers());
		} catch (SchedulerException e) {
			e.printStackTrace();
			logger.info("failed to initialize Trigger Listener");
			logger.trace("", e);
			throw new JobManagerException("failed to initialize Trigger Listener", e);
		}
	}
	private final class FacadeTriggerListener implements TriggerListener {
		private final String name;
		
		public FacadeTriggerListener(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void triggerComplete(Trigger arg0, JobExecutionContext arg1,
				CompletedExecutionInstruction arg2) {
		}

		public void triggerFired(Trigger arg0, JobExecutionContext arg1) {
		}

		public void triggerMisfired(Trigger arg0) {
		}

		public boolean vetoJobExecution(Trigger arg0, JobExecutionContext arg1) {			
			return false;
		}
		
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#createJobClass(java.lang.String, int, long, long)
	 */
	public JobClass createJobClass(String jobClassName, 
			int concurrency, 
			long maxWaitTime, 
			long maxRunTime)  throws JobManagerException{
		logger.trace("JobFacadeImpl.createJobClass(" + jobClassName + ")");
		try {
			JobClassImpl jc = null;
			jc = jobClassMap.get(jobClassName);
			if (jc == null) {
				jc = new JobClassImpl(sched, jobClassName);
				jobClassMap.put(jobClassName, jc);
			}
			return jc;
		} catch (SchedulerException e) {
			logger.info("failed to create JobClass:" + jobClassName, e);
			throw new JobManagerException("failed to create JobClass:" + jobClassName, e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#assignJobClass(java.lang.String, java.lang.String)
	 */
	public void assignJobClass(String jobName, String className)throws JobManagerException {
		logger.info("JobFacadeImpl.assignJob(" + jobName + "," + className + ")");
		JobClassImpl jc = jobClassMap.get(className);
		if (jc != null) {
			jc.addJob(JobKey.jobKey(jobName));
		} else {
			logger.error("no such jobclass(" + className + ") exist");
			throw new JobManagerException("no such jobclass(" + className + ") exist");
		}
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#deleteJobClass(java.lang.String)
	 */
	public void deleteJobClass(String jobClassName)throws JobManagerException {
		logger.trace("JobFacadeImpl.deleteJobClass(" + jobClassName + ")");
		JobClass jc = jobClassMap.get(jobClassName);
		if (jc != null) {
			jc.close();
			jobClassMap.remove(jobClassName);
		} else {
			logger.trace("no such jobclass(" + jobClassName + ")");
			throw new JobManagerException("no such jobclass(" + jobClassName + ")");
		}
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#revokeJobClass(java.lang.String, java.lang.String)
	 */
	public void revokeJobClass(String jobName, String jobClassName) throws JobManagerException {
		logger.info("JobFacadeImpl.revokeJobClass(" + jobName + "," + jobClassName + ")");
		JobClassImpl jc = jobClassMap.get(jobClassName);
		if (jc != null) {
			jc.deleteJob(JobKey.jobKey(jobName));
		} else {
			logger.error("no such jobclass(" + jobClassName + ") exist");
			throw new JobManagerException("no such jobclass(" + jobClassName + ") exist");
		}
	}	
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#createJob(java.lang.String, java.lang.Class, org.quartz.JobDataMap)
	 */
	public JobDetail createJob(String name, Class<? extends Job> cls, JobDataMap data) throws JobManagerException {
		JobDataMap datamap = data;
		if (datamap == null) {
			datamap = new JobDataMap();
		}
		datamap.put(Constants.KEY_PROHIBIT, new HashSet<String>());
		JobDetail job = newJob()
				.withIdentity(name)
				.ofType(cls)
				.storeDurably()
				.requestRecovery(false)
				.usingJobData(datamap)
				.build();

		try {
			sched.addJob(job, true);
		} catch (SchedulerException e) {
			e.printStackTrace();
			logger.info("failed to add job:" + name);
			throw new JobManagerException("failed to add job:" + name, e);
		}
		if (job != null) {
			jobDetailMap.put(name, job);
		}
		return job;
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#createJob(org.wiperdog.jobmanager.JobExecutable)
	 */
	public JobDetail createJob(JobExecutable executable) throws JobManagerException {
		logger.info("--------------------- Start creating job " + executable.getName());		
		String name = executable.getName();
		JobDataMap dataMap = new JobDataMap();
		dataMap.put(Constants.KEY_TYPE, Constants.JOBTYPE_OBJECT);
		dataMap.put(Constants.KEY_OBJECT, executable);

		return createJob(name, ObjectJob.class, dataMap);
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#removeJob(org.quartz.JobDetail)
	 */
	public void removeJob(JobDetail job)  throws JobManagerException {
		logger.info("JobFacadeImpl.removeJob(" + job.getKey().toString() + ")");
		try {
			sched.deleteJob(job.getKey());
			jobDetailMap.remove(job.getKey().getName());
		} catch (SchedulerException e) {
			logger.error("Failed to remove job:" + job.getKey().toString());
			throw new JobManagerException("failed to remove job:" + job.getKey().toString(), e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#createTrigger(java.lang.String)
	 */
	public Trigger createTrigger(String name) throws JobManagerException  {		
		logger.trace("JobFacadeImpl.createTrigger(" + name + ")");
		return newTrigger()
				.withIdentity(autoname(name))
				.startNow()
				.build();
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#scheduleJob(org.quartz.JobDetail, org.quartz.Trigger)
	 */
	public void scheduleJob(JobDetail job, Trigger trigger) throws JobManagerException {
		logger.info("JobFacadeImpl.scheduleJob(" + job.getKey().toString() + "," + trigger.getKey().toString() + ")");
		try {
			TriggerBuilder<? extends Trigger> builder = trigger.getTriggerBuilder();
			Trigger newTrigger = builder.forJob(job).build();
			if (sched.getTrigger(trigger.getKey()) != null) {
				sched.rescheduleJob(trigger.getKey(), newTrigger);
			} else {
				sched.scheduleJob(newTrigger);
			}
		} catch (SchedulerException e) {
			logger.info("Assigning schedule to job failed:" + trigger.getKey().toString() + " --- " + job.getKey().toString(), e);
			throw new JobManagerException("Assigning schedule to job failed:" + trigger.getKey().toString() + " --- " + job.getKey().toString(), e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#unscheduleJob(org.quartz.Trigger)
	 */
	public void unscheduleJob(Trigger trigger) throws JobManagerException {
		logger.info("JobFacadeImpl.unscheduleJob(" + trigger.getKey().toString() + ")");
		try {
			sched.unscheduleJob(trigger.getKey());
		} catch (SchedulerException e) {
			logger.info("failed to unscheduler job:" + trigger.getKey().toString());
			throw new JobManagerException("failed to unscheduler job:" + trigger.getKey().toString(), e);
		}
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#getJob(java.lang.String)
	 */
	public JobDetail getJob(String name) throws JobManagerException {
		logger.info("JobFacadeImpl.getJob(" + name + ")");
		try {
			return sched.getJobDetail(JobKey.jobKey(name));
		} catch (SchedulerException e) {
			logger.info("failed to get job:" + name);
			throw new JobManagerException("failed to get job:" + name, e);
		}
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#getTrigger(java.lang.String)
	 */
	public Trigger getTrigger(String name)  throws JobManagerException {
		logger.info("JobFacadeImpl.getTrigger(" + name + ")");
		try {
			return sched.getTrigger(TriggerKey.triggerKey(name));
		} catch (SchedulerException e) {
			logger.info("failed to get trigger:" + name);
			throw new JobManagerException("failed to get trigger:" + name, e);
		}
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#getJobClass(java.lang.String)
	 */
	public JobClass getJobClass(String name) {
		return jobClassMap.get(name);
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#pauseSchedule()
	 */
	public void pauseSchedule()throws JobManagerException {
		try {
			sched.standby();
		} catch (SchedulerException e) { 
			logger.warn("failed to set scheduler standby mode");
			logger.debug("", e);
			throw new JobManagerException("failed to set scheduler standby mode", e);
		}
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#resumSchedule()
	 */
	public void resumSchedule() throws JobManagerException {
		try {
			sched.start();
		} catch (SchedulerException e) {
			logger.warn("failed to resume scheduler");
			logger.debug("", e);
			throw new JobManagerException("failed to resume scheduler", e);
		}
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#interruptJob(java.lang.String)
	 */
	public boolean interruptJob(String name) throws JobManagerException {		
		try {
			return sched.interrupt(jobKey(name));
		} catch (UnableToInterruptJobException e) {
			logger.info("Failed to interrupt job: " + name, e);			
			throw new JobManagerException("failed to interrupt job: " + name, e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#findJobClassForJob(java.lang.String)
	 */
	public JobClass[] findJobClassForJob(String name) {
		Set<JobClass> jcs = new HashSet<JobClass>();
		Set<String> jckeys = jobClassMap.keySet();
		for (String k : jckeys) {
			JobClass c = jobClassMap.get(k);
			List<JobKey> jklist = c.getAssignedList();
			for (JobKey jk : jklist) {
				if (jk.getName().equals(name)) {
					jcs.add(c);
					break;
				}
			}
		}

		JobClass [] rarray = new JobClass[jcs.size()];
		jcs.toArray(rarray);
		
		return rarray;
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#getJobRunningCount(java.lang.String)
	 */
	public int getJobRunningCount(String name) {
		int count = 0;
		try {
			List<JobExecutionContext> elist = sched.getCurrentlyExecutingJobs();
			for (JobExecutionContext je : elist) {
				String jname = je.getJobDetail().getKey().getName();
				if (jname != null && jname.equals(name)) {
					++count;
				}
			}
		} catch (SchedulerException e) {
		}
		return count;
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#createTrigger(java.lang.String, java.util.Date)
	 */
	public Trigger createTrigger(String name, Date at) {
		return newTrigger()
				.withIdentity(autoname(name))
				.startAt(at)
				.build();
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#revokeJobClass(java.lang.String)
	 */
	public void revokeJobClass(String jobName) {		
		Set<String> jckeys = jobClassMap.keySet();
		
		for (String k : jckeys) {
			JobClass c = jobClassMap.get(k);
			List<JobKey> jklist = c.getAssignedList();
			for (JobKey jk : jklist) {
				if (jk.getName().equals(jobName)) {
					c.deleteJob(jk);					
				}
			}
		}		
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#createTrigger(java.lang.String, int)
	 */
	public Trigger createTrigger(String name, long delay) {
		return newTrigger()
				.withIdentity(autoname(name))
				.startAt(DateBuilder.futureDate((int) delay, IntervalUnit.MILLISECOND))
				.build();
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#createTrigger(java.lang.String, int, long)
	 */
	public Trigger createTrigger(String name, long delay, long interval) {
		logger.trace("JobFacadeImpl.createTrigger(" + name + ", " + delay
				+ ", " + interval + ")");
		Date startTime = new Date(System.currentTimeMillis() + delay);
		Trigger trigger = TriggerBuilder
				.newTrigger()
				.withIdentity(autoname(name))
				.withSchedule(
						SimpleScheduleBuilder.simpleSchedule()
								.withIntervalInMilliseconds(interval)
								.repeatForever()).startAt(startTime).build();
		return trigger;
	}

	/* (non-Javadoc)
	 * @see org.wiperdog.jobmanager.JobFacade#createTrigger(java.lang.String, java.lang.String)
	 */
	public Trigger createTrigger(String name, String crondef) throws JobManagerException {
		logger.info("JobFacadeImpl.createTrigger(" + name + "," + crondef + ")");
		try {
			return newTrigger()
				.withIdentity(autoname(name))
				.withSchedule(
						cronSchedule(crondef)
						)
				.withDescription(crondef)
				.build();
		} catch (Exception e) {
			logger.info("Failed to create cron trigger, bad format:" + name + ", " + crondef, e);
			throw new JobManagerException("Failed to create cron trigger, bad format:" + name + ", " + crondef, e);
		}
	}

}