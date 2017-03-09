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
import java.util.concurrent.ScheduledFuture;

import org.reactivetechnologies.ticker.scheduler.DistributedScheduledTask.TaskContext;
import org.reactivetechnologies.ticker.utils.CommonHelper;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.util.Assert;
/**
 * Abstract {@linkplain ScheduledTask} implementation.
 * @author esutdal
 *
 */
public abstract class AbstractScheduledTask implements ScheduledTask, Runnable{

	private TaskSchedulerService scheduler;
	private DelegatingCronTrigger trigger;
	TaskSchedulerService getScheduler() {
		return scheduler;
	}
	DelegatingCronTrigger getTrigger() {
		return trigger;
	}
	public AbstractScheduledTask() {
		super();
	}
	/**
	 * Package private access. Execute the subclass implemented {@link #run(TaskContext)} method and update trigger last execution timestamp.
	 * @param context
	 */
	void doRun(TaskContext context) {
		run(context);
		trigger.setLastExecutionTime(new Date());
			
	}
	/**
	 * Set the {@linkplain TaskScheduler} instance to this task.
	 * @param scheduler
	 */
	void setScheduler(TaskSchedulerService scheduler) {
		this.scheduler = scheduler;
	}

	/**
	 * Set the {@linkplain CronTrigger} instance to this task.
	 * @param trigger
	 */
	void setTrigger(DelegatingCronTrigger trigger) {
		this.trigger = trigger;
	}

	/**
	 * This is a unique identifier for this {@linkplain ScheduledTask} instance.
	 * @return
	 */
	public String name() {
		return CommonHelper.encodeClassName(getClass())+"__task";
	}
	/**
	 * This method will be invoked after a task is being cancelled.
	 */
	protected void destroy() {
		
	}
	private ScheduledFuture<?> future;

	/**
	 * 
	 * @param cancellable
	 */
	final void setCancellable(ScheduledFuture<?> cancellable) {
		this.future = cancellable;
	}
	public boolean isCancelled()
	{
		Assert.notNull(future, "Not scheduled yet");
		return future.isCancelled();
	}
	public boolean cancel()
	{
		Assert.notNull(future, "Not scheduled yet");
		if(!future.isCancelled())
		{
			boolean b = future.cancel(false);
			destroy();
			return b;
		}
		return false;
		
	}

}