package com.insight_tec.pi.jobmanager.internal;

import java.io.File;
import org.quartz.JobDataMap;
import org.wiperdog.jobmanager.JobExecutable;

public class Common implements JobExecutable {
	String fullPathOfFile;
	String classOfJob; 
	String sender;
	String jobName;
	
	public Common(String fullPathOfFile, String classOfJob, String sender) {
		this.fullPathOfFile = fullPathOfFile;
		this.classOfJob = classOfJob;
		this.sender = sender;
	}
	
	public Object execute(JobDataMap params) throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getName() {
		File f = new File(fullPathOfFile);
		jobName = f.getName();
		return jobName;
	}

	public String getArgumentString() {
		// TODO Auto-generated method stub
		return null;
	}

	public void stop(Thread thread) {
		// TODO Auto-generated method stub
		
	}

}
