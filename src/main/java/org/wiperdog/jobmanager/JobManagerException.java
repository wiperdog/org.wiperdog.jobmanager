package org.wiperdog.jobmanager;

/**
 * Job Manager customized exception class 
 * @author nguyenvannghia
 *
 */
public class JobManagerException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public JobManagerException() {
	}

	/**
	 * 
	 * @param msg
	 */
	public JobManagerException(String msg) {
		super(msg);
	}

	/**
	 * 
	 * @param msg
	 * @param t
	 */
	public JobManagerException(String msg, Throwable t) {
		super(msg, t);
	}

	/**
	 * 
	 * @param t
	 */
	public JobManagerException(Throwable t) {
		super(t);
	}

}