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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public class LongKey implements DataComparable<Long> {

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (value ^ (value >>> 32));
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
		LongKey other = (LongKey) obj;
		if (value != other.value)
			return false;
		return true;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public LongKey() {
	}

	public LongKey(long value) {
		super();
		this.value = value;
	}

	private long value;

	@Override
	public int compareTo(Long o) {
		return Long.compare(value, o);
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeLong(value);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		value = in.readLong();
	}

	@Override
	public Long value() {
		return value;
	}

}
