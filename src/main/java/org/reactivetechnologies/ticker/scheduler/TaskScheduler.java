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

import org.reactivetechnologies.ticker.scheduler.DistributedScheduledTask.TaskContext;
/**
 * 
 * @author esutdal
 *
 */
public interface TaskScheduler {

	/**
	 * Schedule a new recurring task based on a CRON expression. 
	 * Execution will end once the scheduler shuts down or the returned ScheduledFuture gets cancelled.
	 * @param task
	 * @return initial context for the task scheduled.
	 */
	TaskContext scheduleTask(DistributedScheduledTask task);

	/**
	 * Cancel a given task by {@linkplain TaskContext#getKeyParam()}
	 * @param taskId
	 * @param cancelSpawnedTasks
	 * @return
	 */
	boolean cancelTask(TaskContext taskId, boolean cancelSpawnedTasks);

	void destroy();
	/**
	 * Get a cluster synchronized timestamp.
	 * @return
	 */
	Clock getClusterClock();

}