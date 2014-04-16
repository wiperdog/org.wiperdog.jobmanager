/*
 *  Copyright 2013 Insight technology,inc. All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.wiperdog.jobmanager.internal;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;

import java.text.ParseException;
import java.util.ArrayList;
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
import org.quartz.JobBuilder;
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
import org.quartz.impl.matchers.GroupMatcher;
import org.wiperdog.jobmanager.Constants;
import org.wiperdog.jobmanager.JobClass;
import org.wiperdog.jobmanager.JobExecutable;
import org.wiperdog.jobmanager.JobFacade;
import org.wiperdog.jobmanager.JobManagerException;

/**
 * JobFacadeImpl
 * Jobファサード実装
 * 
 * @author kurohara
 *
 */
public class JobFacadeImpl implements JobFacade {
	private final Scheduler sched;
	/*private Map<String,JobNet> jobNetMap = new HashMap<String,JobNet>();*/
	private Map<String,JobClassImpl> jobClassMap = new HashMap<String,JobClassImpl>();
	private Logger logger = Logger.getLogger(JobFacadeImpl.class);
	private Map<String, JobDetail> jobDetailMap = new HashMap<String,JobDetail>();
	/*private Map<String, JobReceiver> jobReceiverMap = new HashMap<String,JobReceiver>();*/
	
	private static long seqnum = 0;
	private static final String AUTONAME_BASE = "JOBOBJECT_";
	
	/*private RShellProvider commander;*/
	
	private int maxReceiveSize = Constants.DEF_MAX_DATA_SIZE;
	private int maxHistoryDepth = Constants.DEF_MAX_HISTORYDEPTH;
	
	/*public void setCommander(RShellProvider commander) {
		this.commander = commander;
	}*/
	
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
	 * 
	 * @author kurohara
	 *
	 */
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
			// TODO Auto-generated method stub
			
		}

		public void triggerFired(Trigger arg0, JobExecutionContext arg1) {
		}

		public void triggerMisfired(Trigger arg0) {
			/*JobReceiver jr = jobReceiverMap.get(arg0.getJobKey().getName());
			jr.putEvent(arg0.getKey().getName(), TriggerReceiver.TRIGGEREVENT.MISFIRED, new Date());*/
		}

		public boolean vetoJobExecution(Trigger arg0, JobExecutionContext arg1) {
			// TODO Auto-generated method stub
			return false;
		}
		
	}
	
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

	public JobClass createJobClass(String name) throws JobManagerException {
		logger.trace("JobFacadeImpl.createJobClass(" + name + ")");
		try {
			JobClassImpl jc = null;
			jc = jobClassMap.get(name);
			if (jc == null) {
				jc = new JobClassImpl(this, sched, name);
				jobClassMap.put(name, jc);
			}
			return jc;
		} catch (SchedulerException e) {
			logger.info("failed to create JobClass:" + name, e);
			throw new JobManagerException("failed to create JobClass:" + name, e);
		}
	}

	public JobClass createJobClass(String name, int concurrency, long maxWaitTime, long maxRunTime) throws JobManagerException {
		logger.trace("JobFacadeImpl.createJobClass(" + name + "," + concurrency + "," + maxWaitTime + "," + maxRunTime + ")");
		try {
			JobClassImpl jc = null;
			jc = jobClassMap.get(name);
			if (jc == null){
				jc = new JobClassImpl(this, sched, name);
				jobClassMap.put(name, jc);
			}
			jc.setConcurrency(concurrency);
			jc.setMaxWaitTime(maxWaitTime);
			jc.setMaxRunTime(maxRunTime);
			return jc;
		} catch (SchedulerException e) {
			logger.info("failed to create JobClass:" + name, e);
			throw new JobManagerException("failed to create JobClass:" + name, e);
		}
	}
	
	public void deleteJobClass(String name) throws JobManagerException {
		logger.trace("JobFacadeImpl.deleteJobClass(" + name + ")");
		JobClass jc = jobClassMap.get(name);
		if (jc != null) {
			jc.close();
			jobClassMap.remove(name);
		} else {
			logger.trace("no such jobclass(" + name + ")");
			throw new JobManagerException("no such jobclass(" + name + ")");
		}
	}
	
	/*private void putJobReceiver(String name, JobReceiverImpl receiver) {
		jobReceiverMap.put(name, receiver);
		jobReceiverMap.put(name + Constants.JOBSUFFIX_INTERRUPTED, receiver.getInterruptedReceiver());
		jobReceiverMap.put(name + Constants.JOBSUFFIX_MISFIRED, receiver.getMisfiredReceiver());
		jobReceiverMap.put(name + Constants.JOBSUFFIX_OUTPATTERN, receiver.getOutPatternReceiver());
		jobReceiverMap.put(name + Constants.JOBSUFFIX_ERRPATTERN, receiver.getErrPatternReceiver());
	}*/
	
	/*private void removeJobReceiver(String name) {
		jobReceiverMap.remove(name);
		jobReceiverMap.remove(name + Constants.JOBSUFFIX_INTERRUPTED);
		jobReceiverMap.remove(name + Constants.JOBSUFFIX_MISFIRED);
		jobReceiverMap.remove(name + Constants.JOBSUFFIX_OUTPATTERN);
		jobReceiverMap.remove(name + Constants.JOBSUFFIX_ERRPATTERN);
	}*/
	
	private JobDetail createJob(String name, Class<? extends Job> cls, JobDataMap data) throws JobManagerException {
		System.out.println("Start creating job....");
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
			// create corresponding jobreceiver here
			/*try {
				JobReceiverImpl receiver = new JobReceiverImpl(sched, jobKey(name), this.maxHistoryDepth);
				putJobReceiver(name, receiver);
			} catch (SchedulerException e) {
				throw new JobManagerException("failed to setup job receiver", e);
			}*/
		}
		return job;
	}
	
	public JobDetail createJob(JobExecutable executable) throws JobManagerException {
		String name = executable.getName();
		JobDataMap dataMap = new JobDataMap();
		dataMap.put(Constants.KEY_TYPE, Constants.JOBTYPE_OBJECT);
		dataMap.put(Constants.KEY_OBJECT, executable);

		return createJob(name, ObjectJob.class, dataMap);
	}
	
	public void removeJob(JobDetail job) throws JobManagerException {
		logger.trace("JobFacadeImpl.removeJob(" + job.getKey().toString() + ")");
		try {
			sched.deleteJob(job.getKey());
			//removeJobReceiver(job.getKey().getName());
			jobDetailMap.remove(job.getKey().getName());
		} catch (SchedulerException e) {
			logger.info("failed to remove job:" + job.getKey().toString());
			throw new JobManagerException("failed to remove job:" + job.getKey().toString(), e);
		}
	}

	/*public List<JobResult> getJobResult(String name) {

		JobReceiver receiver = jobReceiverMap.get(name);
		if (receiver != null && receiver instanceof JobResultSource) {
			List<JobResult> resultlist = receiver.getJobResultList();
			if (resultlist.size() > 0) {
				// check if this job is waiting for concurrency condition be satisfied.
				JobResult jr = resultlist.get(resultlist.size() - 1);
				if (jr.getPendedAt() != null && jr.getEndedAt() == null && jr.getWaitexpiredAt() == null) {
					String jobclassname = (String) jr.getData().get(Constants.KEY_PENDINGJOBCLASS);
					if (jobclassname != null) {
						JobClassImpl jc = (JobClassImpl) getJobClass(jobclassname);
						jc.removeAllExpiredJob();
						// re-get result
						resultlist = receiver.getJobResultList();
					}
				}
			}
			return resultlist;
		}
		return null;
	}*/
	
	public Trigger createTrigger(String name) {
		System.out.println("JobFacadeImpl.createTrigger(" + name + ")");
		logger.trace("JobFacadeImpl.createTrigger(" + name + ")");
		return newTrigger()
				.withIdentity(autoname(name))
				.startNow()
				.build();
		
	}

	public Trigger createTrigger(String name, long delay) {
		System.out.println("JobFacadeImpl.createTrigger(" + name + "," + delay + ")");
		logger.trace("JobFacadeImpl.createTrigger(" + name + "," + delay + ")");
		return newTrigger()
				.withIdentity(autoname(name))
				.startAt(DateBuilder.futureDate((int) delay, IntervalUnit.MILLISECOND))
				.build();
	}

	public Trigger createTrigger(String name, Date at) {
		System.out.println("JobFacadeImpl.createTrigger(" + name + "," + at.toString() + ")");
		logger.trace("JobFacadeImpl.createTrigger(" + name + "," + at.toString() + ")");
		return newTrigger()
				.withIdentity(autoname(name))
				.startAt(at)
				.build();
	}

	public Trigger createTrigger(String name, String crondef) throws JobManagerException {
		System.out.println("JobFacadeImpl.createTrigger(" + name + "," + crondef + ")");
		logger.trace("JobFacadeImpl.createTrigger(" + name + "," + crondef + ")");
		try {
			return newTrigger()
					.withIdentity(autoname(name))
					.withSchedule(
							cronSchedule(crondef)
							)
					.withDescription(crondef)
					.build();
		} catch (ParseException e) {
			logger.info("failed to create cron trigger, bad format:" + name + ", " + crondef, e);
			throw new JobManagerException("failed to create cron trigger, bad format:" + name + ", " + crondef, e);
		}
	}

	public void scheduleJob(JobDetail job, Trigger trigger) throws JobManagerException {
		System.out.println("================== JobFacadeImpl.scheduleJob("
	+ job.getKey().toString() + "," + trigger.getKey().toString() + ")");
		logger.trace("JobFacadeImpl.scheduleJob(" + job.getKey().toString() + "," + trigger.getKey().toString() + ")");
		try {
			TriggerBuilder<? extends Trigger> builder = trigger.getTriggerBuilder();
			Trigger newTrigger = builder.forJob(job).build();
			if (sched.getTrigger(trigger.getKey()) != null) {
				sched.rescheduleJob(trigger.getKey(), newTrigger);
			} else {
				sched.scheduleJob(newTrigger);
			}
		} catch (SchedulerException e) {
			System.out.println("==========assigning schedule to job failed:" + trigger.getKey().toString() + " --- " + job.getKey().toString());
			logger.info("assigning schedule to job failed:" + trigger.getKey().toString() + " --- " + job.getKey().toString(), e);
			throw new JobManagerException("assigning schedule to job failed:" + trigger.getKey().toString() + " --- " + job.getKey().toString(), e);
		}
	}
	
	// 2012-08-07 Luvina Update start
	public Trigger createTrigger(String name, long delay, long interval) {
		System.out.println("JobFacadeImpl.createTrigger(" + name + ", " + delay
				+ ", " + interval + ")");
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

	// 2012-08-07 Luvina Update end

	public Set<TriggerKey> getTriggerKeys() {
		Set<TriggerKey> result = new HashSet<TriggerKey>();
		try {
			List<String> groupNames = sched.getTriggerGroupNames();
			for (String name : groupNames) {
				GroupMatcher<TriggerKey> matcher = GroupMatcher.triggerGroupEquals(name);
				Set<TriggerKey> keys = sched.getTriggerKeys(matcher);
				result.addAll(keys);
			}
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	public Trigger getTrigger(TriggerKey key) {
		try {
			return sched.getTrigger(key);
		} catch (SchedulerException e) {
		}
		return null;
	}
	
	public void unscheduleJob(Trigger trigger) throws JobManagerException {
		logger.trace("JobFacadeImpl.unscheduleJob(" + trigger.getKey().toString() + ")");
		try {
			sched.unscheduleJob(trigger.getKey());
		} catch (SchedulerException e) {
			logger.info("failed to unscheduler job:" + trigger.getKey().toString());
			throw new JobManagerException("failed to unscheduler job:" + trigger.getKey().toString(), e);
		}
	}
	
	public List<Object> getNodeList() {
		logger.trace("JobFacadeImpll.getNodeList()");
		List<Object> list = new ArrayList<Object>();
		// collect global jobs
		Set<String> keys = jobDetailMap.keySet();
		for (String key : keys) {
			list.add(jobDetailMap.get(key));
		}
		return list;
	}

	public JobDetail getJob(String name) throws JobManagerException {
		logger.trace("JobFacadeImpl.getJob(" + name + ")");
		try {
			return sched.getJobDetail(JobKey.jobKey(name));
		} catch (SchedulerException e) {
			logger.info("failed to get job:" + name);
			throw new JobManagerException("failed to get job:" + name, e);
		}
	}

	public Trigger getTrigger(String name) throws JobManagerException {
		logger.trace("JobFacadeImpl.getTrigger(" + name + ")");
		try {
			return sched.getTrigger(TriggerKey.triggerKey(name));
		} catch (SchedulerException e) {
			logger.info("failed to get trigger:" + name);
			throw new JobManagerException("failed to get trigger:" + name, e);
		}
	}

	public void assignJobClass(String jobName, String className) throws JobManagerException {
		logger.trace("JobFacadeImpl.assignJob(" + jobName + "," + className + ")");
		JobClassImpl jc = jobClassMap.get(className);
		if (jc != null) {
			jc.addJob(JobKey.jobKey(jobName));
		} else {
			logger.debug("no such jobclass(" + className + ") exist");
			throw new JobManagerException("no such jobclass(" + className + ") exist");
		}
	}

	public void revokeJobClass(String jobName, String className) throws JobManagerException {
		logger.trace("JobFacadeImpl.revokeJobClass(" + jobName + "," + className + ")");
		JobClassImpl jc = jobClassMap.get(className);
		if (jc != null) {
			jc.deleteJob(JobKey.jobKey(jobName));
		} else {
			logger.debug("no such jobclass(" + className + ") exist");
			throw new JobManagerException("no such jobclass(" + className + ") exist");
		}
	}
	
	public void revokeJobClass(String className) throws JobManagerException {
		logger.trace("JobFacadeImpl.revokeJobClass(" + className + ")");
		JobClassImpl jc = jobClassMap.get(className);
		if (jc != null) {
			List<JobKey> keylist = jc.getAssignedList();
			for (JobKey key : keylist) {
				jc.deleteJob(key);
			}
		} else {
			logger.debug("no such jobclass(" + className + ") exist");
			throw new JobManagerException("no such jobclass(" + className + ") exist");
		}
	}
	
	public Set<String> keySetJob() {
		return jobDetailMap.keySet();
	}

	public Set<String> keySetClass() {
		return jobClassMap.keySet();
	}

	public JobClass getJobClass(String name) {
		return jobClassMap.get(name);
	}

	public boolean interruptJob(String name) throws JobManagerException {
		JobDetail job = getJob(name);
		try {
			return sched.interrupt(jobKey(name));
		} catch (UnableToInterruptJobException e) {
			e.printStackTrace();
			logger.info("failed to interrupt job: " + name );
			logger.debug("", e);
			throw new JobManagerException("failed to interrupt job: " + name, e);
		}
	}
	
	public void pause() throws JobManagerException {
		try {
			sched.standby();
		} catch (SchedulerException e) { 
			logger.warn("failed to set scheduler standby mode");
			logger.debug("", e);
			throw new JobManagerException("failed to set scheduler standby mode", e);
		}
	}

	public void resume() throws JobManagerException {
		try {
			sched.start();
		} catch (SchedulerException e) {
			logger.warn("failed to resume scheduler");
			logger.debug("", e);
			throw new JobManagerException("failed to resume scheduler", e);
		}
	}

	public JobKey jobKeyForName(String name) {
		return jobKey(name);
	}

	public TriggerKey triggerKeyForName(String name) {
		return triggerKey(name);
	}
	
	public void setJobLastingTime(String name, long timelength) throws JobManagerException {
		logger.trace("setJobLastingTime(" + name + "," + timelength +")");
		Long oTime = (timelength < 0 ? Long.MAX_VALUE : Long.valueOf(timelength));
		try {
			JobDetail job = sched.getJobDetail(jobKey(name));
			if (job != null) {
				JobDataMap data = job.getJobDataMap();
				data.put(Constants.KEY_MAXRUNTIME, oTime);
				sched.addJob(job, true);
			} else  {
				logger.trace("no such job(" + name + ") to set lasting time");
				throw new JobManagerException("no such job(" + name + ") to set lasting time");
			}
		} catch (SchedulerException e) {
			logger.warn("failed to set job lasting length(time)");
			logger.debug("", e);
			throw new JobManagerException("failed to set job lasting length(time)", e);
		}
	}

	/*public JobReceiver getJobReceiver(String name) {
		return jobReceiverMap.get(name);
	}*/
	
	public void triggerJobNondurably(JobDetail job, Trigger trigger) throws JobManagerException {
		try {
			sched.deleteJob(job.getKey());
			JobBuilder builder = job.getJobBuilder();
			JobDetail jobToKick = builder.storeDurably(false)
			.build();
			Trigger jobKicker = trigger.getTriggerBuilder()
					.forJob(jobToKick)
					.build();
			sched.scheduleJob(jobToKick, jobKicker);
			
		} catch (SchedulerException e) {
			logger.warn("failed to trigger job :" + job.getKey().getName());
			logger.trace("", e);
			throw new JobManagerException("failed to trigger job :" + job.getKey().getName(), e);
		}
	}

	public Set<String> getRunningJobSet() {
		Set<String> jobNameList = new HashSet<String>();;
		try {
			List<JobExecutionContext> elist = sched.getCurrentlyExecutingJobs();
			for (JobExecutionContext je : elist) {
				jobNameList.add(je.getJobDetail().getKey().getName());
			}
		} catch (SchedulerException e) {
		}
		return jobNameList;
	}

	public long getJobNextFireLatency(String jobname) throws JobManagerException {
		List<Trigger> tlist = getRelatedTrigger(jobname);
		// 最小の待ち時間(ミリ秒）を返却する。
		long minlatency = -1;
		long now = (new Date()).getTime();
		for (Trigger t : tlist) {
			Date next = t.getNextFireTime();
			if (next != null){
				long latency = next.getTime() - now;
				if (minlatency < 0L) {
					minlatency = latency;
				} else if (latency < minlatency) {
					minlatency = latency;
				}
			}
		}
		return minlatency;
	}
	
	@SuppressWarnings("unchecked")
	public List<Trigger> getRelatedTrigger(String jobname) throws JobManagerException {
		try {
			return (List<Trigger>) sched.getTriggersOfJob(jobKeyForName(jobname));
		} catch (SchedulerException e) {
			throw new JobManagerException("failed to retrieve trigger for job: " + jobname, e);
		}
	}
	
	public int getJobRunningCount(String jobname) {
		int count = 0;
		try {
			List<JobExecutionContext> elist = sched.getCurrentlyExecutingJobs();
			for (JobExecutionContext je : elist) {
				String jname = je.getJobDetail().getKey().getName();
				if (jname != null && jname.equals(jobname)) {
					++count;
				}
			}
		} catch (SchedulerException e) {
		}
		return count;
	}

	public void setMaxReceiveSize(int size) {
		this.maxReceiveSize = size;
	}

	public void setMaxHistoryDepth(int maxDepth) {
		maxHistoryDepth = maxDepth;
	}

	public void setJobDataReceiveSize(String name, int size)
			throws JobManagerException {
		JobDetail job = null;
		try {
			job = sched.getJobDetail(jobKeyForName(name));
			if (job != null) {
				job.getJobBuilder().usingJobData("maxDataSize", Integer.valueOf(size));
				sched.addJob(job, true);
				return;
			}
		} catch (SchedulerException e) {
			throw new JobManagerException("failed to get job detail", e);
		}
		throw new JobManagerException("no such job exist(" + name +")");
	}
	
	public int getJobDataReceiveSize(String name) throws JobManagerException {
		JobDetail job = jobDetailMap.get(name);
		int maxDataSize = this.maxReceiveSize;
		if (job != null) {
			try {
				Integer v = (Integer) job.getJobDataMap().get("maxDataSize");
				if (v != null) {
					maxDataSize = v;
				}
			} catch (Exception e) {
				throw new JobManagerException("failed to get property", e);
			}
		} else {
			throw new JobManagerException("no such job exist(" + name + ")");
		}
		
		return maxDataSize;
	}

	/*public void setJobHistoryLength(String name, int length)
			throws JobManagerException {
		JobReceiverImpl receiver = (JobReceiverImpl) jobReceiverMap.get(name);
		if (receiver != null) {
			receiver.setHistorySize(length);
		} else {
			throw new JobManagerException("no such job exist(" + name + ")");
		}
	}

	public int getJobHistoryLength(String name) throws JobManagerException {
		JobReceiverImpl receiver = (JobReceiverImpl) jobReceiverMap.get(name);
		if (receiver != null) {
			return receiver.getHistorySize();
		} else {
			throw new JobManagerException("no such job exist (" + name + ")");
		}
	}*/

	public JobClass[] findJobClassForJob(String jobName) {
		Set<JobClass> jcs = new HashSet<JobClass>();
		Set<String> jckeys = jobClassMap.keySet();
		for (String k : jckeys) {
			JobClass c = jobClassMap.get(k);
			List<JobKey> jklist = c.getAssignedList();
			for (JobKey jk : jklist) {
				if (jk.getName().equals(jobName)) {
					jcs.add(c);
					break;
				}
			}
		}

		JobClass [] rarray = new JobClass[jcs.size()];
		jcs.toArray(rarray);
		
		return rarray;
	}

	public Object getSchedulerObject() {
		return sched;
	}
}
