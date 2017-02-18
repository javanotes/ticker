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

import java.util.Date;
/**
 * @experimental
 * @author esutdal
 *
 */
public abstract class SpawnedScheduledTask extends AbstractScheduledTask {

	/**
	 * This time represents the actual execution time of the schedule, after loading has been done.
	 * So the prepare phase will run at {@link #cronExpression()} intervals, and the actual executions
	 * will run at + delta {@linkplain DelayTime}.
	 * @return
	 */
	public abstract Clock executeAfter();
	
	/**
	 * The prepare method to be run. This is run at cron schedule and before the actual execution.
	 * @return
	 */
	public abstract TaskContext runPrepare();
	/**
	 * The actual execution of scheduled task.
	 * @return
	 */
	public abstract TaskContext runExecute();
	
	
	@Override
	public TaskContext run(TaskContext context) {
		System.err.println(new Date()+", ["+Thread.currentThread().getName()+"] - DistributedScheduledTask.run()");
		return context;
	}
	protected SpawnedScheduledTask spawnTask(TaskContext context)
	{
		return this;
	}
}
