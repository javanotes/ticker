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
package ticker.scheduler;

import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.scheduler.Clock;
import org.reactivetechnologies.ticker.scheduler.DistributedScheduledTask.TaskContext;
import org.reactivetechnologies.ticker.scheduler.SpawnedScheduledTask;

public class MyScheduledChildTask extends SpawnedScheduledTask {

	public MyScheduledChildTask() {
		super();
	}

	@Override
	public void run(TaskContext context) {
		System.err.println(new Date()+", ["+Thread.currentThread().getName()+"] - MyScheduledChildTask.run()");
		String s = (String) context.getTransient(MyScheduledTask.someKey);
		System.err.println(new Date()+", ["+Thread.currentThread().getName()+"] - Got transient: "+s);
		
		Data d = context.get(MyScheduledTask.someKey);
		System.err.println(new Date()+", ["+Thread.currentThread().getName()+"] - Got data: "+d);
	}

	@Override
	public Clock executeAfter() {
		return new Clock(1, TimeUnit.SECONDS, TimeZone.getDefault());
	}
	
}
