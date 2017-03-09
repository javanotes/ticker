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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A special {@linkplain ScheduledTask} that is run as a 'post scenario' of an {@linkplain DistributedScheduledTask}.
 * Thus it behaves as a child task, which will be run on a schedule with a 'happens-after' guarantee. It will share the same context as used by the parent {@linkplain DistributedScheduledTask}, thus
 * leveraging a post task handling. <p>Note: This type of task is, however, NOT distributed. This task is run locally (no cluster competition), on the node that has run the 
 * parent {@linkplain DistributedScheduledTask}.
 * @author esutdal
 *
 */
public abstract class SpawnedScheduledTask extends AbstractScheduledTask {

	private static final Logger log = LoggerFactory.getLogger(SpawnedScheduledTask.class);
	private volatile TaskContext passedContext;
	/**
	 * Represents the execution time of the schedule, after the execution of the parent {@linkplain DistributedScheduledTask} .
	 * So the execution phase will run at ({@link DistributedScheduledTask#cronExpression() parent_schedule}  + executeAfter()) intervals.
	 * @return
	 * @see SpawnedScheduledTask#cronExpression()
	 */
	public abstract Clock executeAfter();
		
	/**
	 * By default the {@link #executeAfter()} is looked up in case of {@linkplain SpawnedScheduledTask} instances.
	 * However, if this method is implemented with a valid CRON expression, it will override {@linkplain #executeAfter()}.
	 */
	@Override
	public String cronExpression()
	{
		return null;
		
	}
	@Override
	public void run() {
		try 
		{
			doRun(passedContext);
		} 
		catch (Exception e) {
			log.error("Internal Error!", e);
		}
	}
	
	/**
	 * Used internally for passing {@linkplain TaskContext} from the parent {@linkplain DistributedScheduledTask}.
	 * @param passedContext
	 */
	final void setPassedContext(TaskContext passedContext) {
		this.passedContext = passedContext;
	}
		
}
