package org.reactivetechnologies.ticker.messaging;

import org.reactivetechnologies.ticker.messaging.Data;

public class MessageProcessingException extends Exception {

	private Data record;
	public MessageProcessingException(String msg, Throwable cause) {
		super(msg, cause);
	}
	public MessageProcessingException(Throwable cause) {
		super("", cause);
	}
	public Data getRecord() {
		return record;
	}
	public void setRecord(Data record) {
		this.record = record;
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
