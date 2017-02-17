/* ============================================================================
*
* FILE: Message.java
*
The MIT License (MIT)

Copyright (c) 2016 Sutanu Dalui

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*
* ============================================================================
*/
package org.reactivetechnologies.ticker.messaging.data;

import java.io.IOException;

import org.reactivetechnologies.ticker.messaging.Data;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

/**
 * Simplest form of {@linkplain Data} that encapsulates utf-8 strings.
 */
public class ByteData extends Data {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6294748301538391817L;

	/**
	 * Default constructor.
	 */
	public ByteData() {
		super();
	}

	/**
	 * New message with a UTF8 encoded payload.
	 * 
	 * @param s
	 */
	public ByteData(byte[] payload) {
		setPayload(payload);
	}

	/**
	 * New message with a UTF8 encoded payload and destination queue.
	 * 
	 * @param payload
	 * @param destination
	 */
	public ByteData(byte[] payload, String destination) {
		this(payload);
		setDestination(destination);
	}

	/**
	 * New message with a UTF8 encoded payload, destination queue and a
	 * correlation ID.
	 * 
	 * @param payload
	 * @param destination
	 * @param corrID
	 */
	public ByteData(byte[] payload, String destination, String corrID) {
		this(payload, destination);
		setCorrelationID(corrID);
	}

	public byte[] getPayload() {
		return payload;
	}

	public void setPayload(byte[] payload) {
		this.payload = payload;
	}

	private byte[] payload;
	
	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		super.writeData(out);
		if(payload != null){
			out.writeInt(payload.length);
			out.write(payload);
		}
		else
			out.writeInt(-1);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		super.readData(in);
		int i = in.readInt();
		if(i != -1){
			byte[] b = new byte[i];
			in.readFully(b);
			setPayload(b);
		}
	}

}
