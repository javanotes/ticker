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

import org.reactivetechnologies.ticker.scheduler.TaskScheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
@Component
public class Runner implements CommandLineRunner {

	@Autowired
	TaskScheduler scheduler;
	@Override
	public void run(String... args) throws Exception {
		MyScheduledTask task = new MyScheduledTask("*/5 * * * * *");
		task.setChildTask(MyScheduledChildTask.class);
		scheduler.scheduleTask(task);
		System.err.println(new Date()+", ["+Thread.currentThread().getName()+"] - Registered task ...");
	}

}
