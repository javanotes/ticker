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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.data.TextData;
import org.reactivetechnologies.ticker.scheduler.DistributedScheduledTask;

public class MyScheduledTask extends DistributedScheduledTask {

	public static final String someKey = "SOME_KEY";
	public MyScheduledTask(String cronExpr) {
		super();
		this.cronExpr = cronExpr;
	}

	private String cronExpr;
	@Override
	public void run(TaskContext context) {
		context.spawnChildTask(true);
		String valu = UUID.randomUUID().toString();
		if (!context.containsKeyTransient(someKey)) {
			context.setTransient(someKey, valu);
			System.err.println(new Date()+", ["+Thread.currentThread().getName()+"] - Have set transient: " + valu);
		}
		if (!context.containsKey(someKey)) {
			Data d = new TextData(valu);
			context.put(someKey, d);
			System.err.println(new Date()+", ["+Thread.currentThread().getName()+"] - Have set data: " + d);
		}
		System.err.println(new Date()+", ["+Thread.currentThread().getName()+"] - DistributedScheduledTask.run()");
		//context.emit(d);
	}

	@Override
	public String cronExpression() {
		return cronExpr;
	}

	@Override
	protected TimeUnit scheduleTimeunit() {
		return TimeUnit.SECONDS;
	}
	
}
