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
package org.reactivetechnologies.ticker.scheduler;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.reactivetechnologies.ticker.messaging.Data;

public class TaskContext implements Serializable {
	/**
	 * Create a new instance with the same key.
	 * @return
	 */
	public TaskContext copy()
	{
		return new TaskContext(getKeyParam());
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 * @param keyParam
	 */
	protected TaskContext(String keyParam) {
		super();
		this.keyParam = keyParam;
	}

	private boolean emitData;
	private final List<Data> dataSet = new LinkedList<>();
	//Add more params as needed
	private final String keyParam;

	/**
	 * Getter for keyParam. This will correspond to one unique {@linkplain AbstractScheduledTask}.
	 * @return The key for this context
	 */
	public String getKeyParam() {
		return keyParam;
	}

	public void addData(Data d)
	{
		dataSet.add(d);
	}
	public Iterator<? extends Data> getDataSet() {
		return dataSet.iterator();
	}
	public void clearDataSet() {
		dataSet.clear();
	}

	public boolean isEmitData() {
		return emitData;
	}

	public void setEmitData(boolean emitData) {
		this.emitData = emitData;
	}

}
