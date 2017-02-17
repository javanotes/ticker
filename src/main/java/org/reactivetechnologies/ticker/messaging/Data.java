package org.reactivetechnologies.ticker.messaging;

import java.io.IOException;
import java.io.Serializable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.UuidUtil;

public abstract class Data implements DataSerializable, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String correlationID = "";
	private long timestamp = 0;
	private String destination = "";
	private String replyTo = "";
	private boolean redelivered = false;
	private long expiryMillis = 0;
	private short redeliveryCount = 0;
	
	private transient boolean addAsync = false;
	
	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(correlationID);
		out.writeUTF(destination);
		out.writeUTF(replyTo);
		out.writeLong(expiryMillis);
		out.writeLong(timestamp);
		out.writeBoolean(redelivered);
		out.writeShort(redeliveryCount);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		setCorrelationID(in.readUTF());
		setDestination(in.readUTF());
		setReplyTo(in.readUTF());
		setExpiryMillis(in.readLong());
		setTimestamp(in.readLong());
		setRedelivered(in.readBoolean());
		setRedeliveryCount(in.readShort());
	}
		
	public String getCorrelationID() {
		return correlationID;
	}

	public void setCorrelationID(String correlationID) {
		this.correlationID = correlationID;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getReplyTo() {
		return replyTo;
	}

	public void setReplyTo(String replyTo) {
		this.replyTo = replyTo;
	}

	public boolean isRedelivered() {
		return redelivered;
	}

	public void setRedelivered(boolean redelivered) {
		this.redelivered = redelivered;
	}

	public long getExpiryMillis() {
		return expiryMillis;
	}

	public void setExpiryMillis(long expiryMillis) {
		this.expiryMillis = expiryMillis;
	}
	/**
	 * 
	 */
	public Data() {
		setTimestamp(System.currentTimeMillis());
		setCorrelationID(UuidUtil.createClusterUuid());
	}

	public short getRedeliveryCount() {
		return redeliveryCount;
	}

	public void setRedeliveryCount(short redeliveryCount) {
		this.redeliveryCount = redeliveryCount;
	}

	public boolean isAddAsync() {
		return addAsync;
	}

	public void setAddAsync(boolean addAsync) {
		this.addAsync = addAsync;
	}
	
}