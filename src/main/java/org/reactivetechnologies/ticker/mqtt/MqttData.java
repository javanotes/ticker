/**
 * Copyright 2017 esutdal

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package org.reactivetechnologies.ticker.mqtt;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.reactivetechnologies.ticker.messaging.data.ByteData;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.parser.proto.messages.AbstractMessage.QOSType;

public class MqttData extends ByteData {

	public long toLong()
	{
		return ByteBuffer.wrap(getPayload()).order(ByteOrder.BIG_ENDIAN).asLongBuffer().get();
	}
	public int toInt()
	{
		return ByteBuffer.wrap(getPayload()).order(ByteOrder.BIG_ENDIAN).asIntBuffer().get();
	}
	public double toDouble()
	{
		return ByteBuffer.wrap(getPayload()).order(ByteOrder.BIG_ENDIAN).asDoubleBuffer().get();
	}
	public String toUtf8()
	{
		return new String(getPayload(), StandardCharsets.UTF_8);
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 4585074864551171836L;

	private static byte[] toArray(ByteBuffer buff)
	{
		//not flipping
		byte[] b = new byte[buff.limit()];
		buff.get(b);
		return b;
	}
	private String topicName, clientId, userName;
	private boolean dupFlag, retainFlag;
	private QOSType qos;
	public boolean isDupFlag() {
		return dupFlag;
	}

	public void setDupFlag(boolean dupFlag) {
		this.dupFlag = dupFlag;
	}

	public boolean isRetainFlag() {
		return retainFlag;
	}

	public void setRetainFlag(boolean retainFlag) {
		this.retainFlag = retainFlag;
	}

	public QOSType getQos() {
		return qos;
	}

	public void setQos(QOSType qos) {
		this.qos = qos;
	}

	@Override
	public String toString() {
		return "MqttData [topicName=" + topicName + ", clientId=" + clientId + ", userName=" + userName + ", dupFlag="
				+ dupFlag + ", retainFlag=" + retainFlag + ", qos=" + qos + "]";
	}

	public MqttData(InterceptPublishMessage mqttData) {
		super();
		setTopicName(mqttData.getTopicName());
		setDestination(getTopicName());
		setClientId(mqttData.getClientID());
		setUserName(mqttData.getUsername());
		setPayload(toArray(mqttData.getPayload()));
		setDupFlag(mqttData.isDupFlag());
		setRetainFlag(mqttData.isRetainFlag());
		setQos(mqttData.getQos());
	}

	public MqttData() {
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		super.readData(in);
		setTopicName(in.readUTF());
		setClientId(in.readUTF());
		setUserName(in.readUTF());
		setDupFlag(in.readBoolean());
		setRetainFlag(in.readBoolean());
		setQos(QOSType.valueOf(in.readByte()));
	}
	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		super.writeData(out);
		out.writeUTF(topicName);
		out.writeUTF(clientId);
		out.writeUTF(userName);
		out.writeBoolean(dupFlag);
		out.writeBoolean(retainFlag);
		out.writeByte(qos.byteValue());
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

}
