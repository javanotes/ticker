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
package org.reactivetechnologies.ticker.messaging.data;

import java.io.Serializable;

import org.reactivetechnologies.ticker.messaging.Data;
/**
 * Wrapper over {@linkplain Data}, to be used internally.
 * @author esutdal
 *
 */
public class DataWrapper 
{
	private boolean removeImmediate;
	/**
	 * 
	 * @param data
	 * @param commit
	 * @param key
	 */
	public DataWrapper(Data data, boolean commit, Serializable key) {
		this(data, commit, key, 0);
	}
	public DataWrapper increment()
	{
		return new DataWrapper(data, commit, key, deliveryCount+1);
	}
	public final Data data;
	public final boolean commit;
	public final Serializable key;
	public final int deliveryCount;
	/**
	 * 
	 * @param data
	 * @param commit
	 * @param key
	 * @param deliveryCount
	 */
	public DataWrapper(Data data, boolean commit, Serializable key, int deliveryCount) {
		super();
		this.data = data;
		this.commit = commit;
		this.key = key;
		this.deliveryCount = deliveryCount;
	}
	public boolean isRemoveImmediate() {
		return removeImmediate;
	}
	public void setRemoveImmediate(boolean removeImmediate) {
		this.removeImmediate = removeImmediate;
	}
		
}