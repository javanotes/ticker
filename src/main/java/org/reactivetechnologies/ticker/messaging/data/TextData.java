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
public class TextData extends Data {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6294748301538391817L;

	/**
	 * Default constructor.
	 */
	public TextData() {
		super();
	}

	/**
	 * New message with a UTF8 encoded payload.
	 * 
	 * @param s
	 */
	public TextData(String payload) {
		this();
		setPayload(payload);
	}

	/**
	 * New message with a UTF8 encoded payload and destination queue.
	 * 
	 * @param payload
	 * @param destination
	 */
	public TextData(String payload, String destination) {
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
	public TextData(String payload, String destination, String corrID) {
		this(payload, destination);
		setCorrelationID(corrID);
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	private String payload;
	
	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		super.writeData(out);
		out.writeUTF(payload);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		super.readData(in);
		setPayload(in.readUTF());
	}

}
