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

import java.util.concurrent.TimeUnit;

/**
 * A special {@linkplain ScheduledTask} that is run as a 'post scenario' of an {@linkplain AbstractScheduledTask}.
 * Thus it behaves as a child task. It will share the same context as used by the parent {@linkplain AbstractScheduledTask}, thus
 * leveraging a pre-post type task handling.
 * @author esutdal
 *
 */
public abstract class SpawnedScheduledTask extends AbstractScheduledTask {

	private volatile TaskContext passedContext;
	/**
	 * This time represents the actual execution time of the schedule, after loading has been done.
	 * So the prepare phase will run at {@link #cronExpression()} intervals, and the actual executions
	 * will run at + delta.
	 * @return
	 */
	public abstract Clock executeAfter();
	/**
	 * This method is overridden so that a single spawned child task keeps recurring, once started 
	 * by the parent task. Do not modify this, else there can be chances of multiple child schedule runs.
	 * @see AbstractScheduledTask#spawnTask(TaskContext)
	 */
	protected final SpawnedScheduledTask spawnTask(TaskContext context)
	{
		return null;
	}
	/**
	 * This method is overridden and set to true. The spawned task can only run on the instance 
	 * that acquired the parent task. So no need to check here anymore.
	 */
	@Override
	final boolean acquireLock() 
	{
		return true;
	}
	public String cronExpression()
	{
		return null;
		
	}
	protected final TimeUnit scheduleTimeunit() {
		//noop
		throw new UnsupportedOperationException();
	}
	@Override
	TaskContext getPassedContext() {
		return passedContext;
	}
	void setPassedContext(TaskContext passedContext) {
		this.passedContext = passedContext;
	}
}
