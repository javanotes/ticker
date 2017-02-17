/**
 * Copyright 2016 esutdal

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
package org.reactivetechnologies.ticker.messaging.data;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
/**
 * Extends {@linkplain TextData} to store an object in its JSON form. 
 * @author esutdal
 *
 * @param <T>
 */
public class ObjectData<T> extends TextData {

	public T getObject() {
		if(object == null){
			try {
				setObject(om.reader().readValue(getPayload()));
			} catch (IOException e) {
				throw new IllegalArgumentException("Exception while deserializing from JSON", e);
			}
		}
		
		return object;
	}

	public void setObject(T object) {
		this.object = object;
	}

	private transient T object;
	private final ObjectMapper om = new ObjectMapper();
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 */
	public ObjectData() {
		super();
	}
	/**
	 * 
	 * @param object
	 */
	public ObjectData(T object) {
		this();
		this.object = object;
		try 
		{
			String json = om.writer().writeValueAsString(object);
			setPayload(json);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Exception while serializing to JSON", e);
		}
	}
	/**
	 * 
	 * @param object
	 * @param destination
	 */
	public ObjectData(T object, String destination) {
		this(object);
		setDestination(destination);
	}
	/**
	 * 
	 * @param object
	 * @param destination
	 * @param corrID
	 */
	public ObjectData(T object, String destination, String corrID) {
		this(object, destination);
		setCorrelationID(corrID);
	}

}
