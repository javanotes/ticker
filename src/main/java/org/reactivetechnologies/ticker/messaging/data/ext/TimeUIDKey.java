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
import java.nio.ByteBuffer;
import java.util.UUID;

import org.reactivetechnologies.ticker.messaging.data.DataComparable;
import org.reactivetechnologies.ticker.utils.TimeUIDSupport;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public class TimeUIDKey implements DataComparable<UUID> {

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		byte[] b = TimeUIDSupport.decompose(value);
		if(b != null)
		{
			out.writeInt(b.length);
			out.write(b);
		}
		else
			out.writeInt(-1);
	}

	//http://grepcode.com/file_/repo1.maven.org/maven2/org.apache.cassandra/cassandra-all/2.0.0/org/apache/cassandra/db/marshal/TimeUUIDType.java/?v=source
	private int compareTo(ByteBuffer o1, ByteBuffer o2)
    {
        if (o1.remaining() == 0)
        {
            return o2.remaining() == 0 ? 0 : -1;
        }
        if (o2.remaining() == 0)
        {
            return 1;
        }
        int res = compareTimestampBytes(o1, o2);
        if (res != 0)
            return res;
        return o1.compareTo(o2);
    }

    private static int compareTimestampBytes(ByteBuffer o1, ByteBuffer o2)
    {
        int o1Pos = o1.position();
        int o2Pos = o2.position();

        int d = (o1.get(o1Pos + 6) & 0xF) - (o2.get(o2Pos + 6) & 0xF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 7) & 0xFF) - (o2.get(o2Pos + 7) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 4) & 0xFF) - (o2.get(o2Pos + 4) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 5) & 0xFF) - (o2.get(o2Pos + 5) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos) & 0xFF) - (o2.get(o2Pos) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 1) & 0xFF) - (o2.get(o2Pos + 1) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 2) & 0xFF) - (o2.get(o2Pos + 2) & 0xFF);
        if (d != 0) return d;

        return (o1.get(o1Pos + 3) & 0xFF) - (o2.get(o2Pos + 3) & 0xFF);
    }
    
	@Override
	public void readData(ObjectDataInput in) throws IOException {
		int len = in.readInt();
		if(len != -1)
		{
			byte[] b = new byte[len];
			in.readFully(b);
			value = TimeUIDSupport.getUUID(ByteBuffer.wrap(b));
		}
		
	}

	public TimeUIDKey() {
	}

	public TimeUIDKey(UUID value) {
		super();
		this.value = value;
	}

	private UUID value;

	public void setValue(UUID value) {
		this.value = value;
	}

	@Override
	public UUID value() {
		return value;
	}

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
		TimeUIDKey other = (TimeUIDKey) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public int compareTo(DataComparable<UUID> other) {
		return compareTo(ByteBuffer.wrap(TimeUIDSupport.decompose(value)), ByteBuffer.wrap(TimeUIDSupport.decompose(other.value())));
	}

}
