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
package org.reactivetechnologies.ticker.messaging.data.ext;

import java.io.IOException;

import org.reactivetechnologies.ticker.messaging.data.DataComparable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public class StringKey implements DataComparable<String> {

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StringKey other = (StringKey) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	private String value;
	public StringKey() {
	}
	/**
	 * 
	 * @param value
	 */
	public StringKey(String value) {
		super();
		this.value = value;
	}
	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(value);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		value = in.readUTF();
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String value() {
		return value;
	}
	
	@Override
	public int compareTo(DataComparable<String> o) {
		if(this.value == null && o != null)
			return 1;
		if(o == null)
			return -1;
		return this.value.compareTo(o.value());
	}

}
