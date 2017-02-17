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

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.springframework.util.Assert;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class Clock implements DataSerializable {
	public Clock() {
		
	}
	
	public Clock(long timestamp, TimeUnit unit, TimeZone zone) {
		super();
		this.timestamp = timestamp;
		this.unit = unit;
		this.zone = zone;
	}

	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * Convert to a timestamp string, timing up till {@linkplain TimeUnit} in HOUR/MINUTE/SECONDS only.
	 * @param tillUnit
	 * @return
	 */
	public String toTimestampString(TimeUnit tillUnit)
	{
		boolean checkVal = tillUnit != null && (tillUnit == TimeUnit.HOURS || tillUnit == TimeUnit.MINUTES || tillUnit == TimeUnit.SECONDS);
		Assert.isTrue(checkVal, "Invalid time in "+tillUnit);
		
		Calendar c = new GregorianCalendar(zone);
		c.setTimeInMillis(timestamp);
		
		StringBuilder s = new StringBuilder();
		s.append(c.get(Calendar.YEAR))
		.append("_")
		.append(c.get(Calendar.DAY_OF_YEAR))
		.append("_")
		.append(c.get(Calendar.HOUR_OF_DAY));
		
		if (tillUnit == TimeUnit.MINUTES || tillUnit == TimeUnit.SECONDS) {
			s.append("_").append(c.get(Calendar.MINUTE));
		}
		if (tillUnit == TimeUnit.SECONDS) {
			s.append("_").append(c.get(Calendar.SECOND));
		}
		return s.toString();
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public TimeUnit getUnit() {
		return unit;
	}

	public void setUnit(TimeUnit unit) {
		this.unit = unit;
	}

	public TimeZone getZone() {
		return zone;
	}

	public void setZone(TimeZone zone) {
		this.zone = zone;
	}

	static final String NIL = "_";
	private long timestamp;
	private TimeUnit unit;
	private TimeZone zone;

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeLong(timestamp);
		out.writeUTF(unit != null ? unit.name() : NIL);
		out.writeUTF(zone != null ? zone.getID() : NIL);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		setTimestamp(in.readLong());
		String s = in.readUTF();
		if (!NIL.equals(s))
			unit = TimeUnit.valueOf(s);
		s = in.readUTF();
		if (!NIL.equals(s))
			zone = TimeZone.getTimeZone(s);
	}

	public Date toDate() {
		return new Date(timestamp);
	}

}
