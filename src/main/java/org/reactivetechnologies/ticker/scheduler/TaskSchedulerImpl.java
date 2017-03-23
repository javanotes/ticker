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
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.reactivetechnologies.ticker.datagrid.HazelcastOperations;
import org.reactivetechnologies.ticker.messaging.base.Publisher;
import org.reactivetechnologies.ticker.scheduler.DistributedScheduledTask.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;

import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
/**
 * The core service class for scheduling tasks.
 * @author esutdal
 *
 */
class TaskSchedulerImpl implements TaskScheduler {

	private static final Logger log = LoggerFactory.getLogger(TaskSchedulerImpl.class);
	private final ConcurrentMap<String, AbstractScheduledTask> registry = new ConcurrentHashMap<>();
	
	@Autowired
	private ThreadPoolTaskScheduler delegate;
	@Autowired 
	private Publisher pub;
	@Autowired
	private HazelcastOperations opsFact;
	
	/**
	 * Singleton instance of {@linkplain HazelcastOperations}.
	 * @return {@linkplain HazelcastOperations}
	 */
	public HazelcastOperations getHazelcastOps()
	{
		return opsFact;
	}
	
	@PostConstruct
	private void init()
	{
		if(isAvailable())
		{
			synchronizeClock();
			log.info("Scheduler Manager initialization complete");
		}
		else
			throw new BeanCreationException("Hazelcast is not active");
	}
	private void synchronizeClock() {
		IMap<String, Clock> map = getHazelcastOps().getMap(getClass().getName()+"_clock");
		if(!map.containsKey("clusterClock"))
		{
			map.lock("clusterClock");
			try {
				clusterClock = new Clock(System.currentTimeMillis(), TimeUnit.MILLISECONDS, TimeZone.getDefault());
				map.putIfAbsent("clusterClock", clusterClock);
			} finally {
				map.unlock("clusterClock");
			}
		}
		clusterClock = map.get("clusterClock");
		
		//if offset is -ve, implies this clock is lagging behind the cluster clock.
		boolean intrr = false;
		clockOffset = (System.currentTimeMillis() - clusterClock.getTimestamp());
		//Assert.isTrue(clockOffset >= 0, "This instance is lagging behind cluster clock");
		if(clockOffset < 0)
		{
			clockOffset = Math.abs(clockOffset);
			try {
				Thread.sleep(clockOffset);
			} catch (InterruptedException e) {
				intrr = true;
			}
			clockOffset += (System.currentTimeMillis() - clusterClock.getTimestamp());
		}
		if(intrr)
			Thread.currentThread().interrupt();
		
		log.info("Clock offset: "+clockOffset+" Cluster Epoch: "+new Date(clusterClock.getTimestamp()));
	}

	private volatile long clockOffset;
	
	/**
	 * Get a cluster synchronized timestamp.
	 * @return
	 */
	@Override
	public Clock getClusterClock() {
		Assert.notNull(clusterClock);
		ILock l = getHazelcastOps().getClusterLock("CLOCK");
		l.lock();
		try 
		{
			return new Clock(System.currentTimeMillis() - clockOffset, clusterClock.getUnit(),
					clusterClock.getZone());
		} finally {
			l.unlock();
		}
	}

	private Clock clusterClock;
	/**
	 * 
	 * @return
	 */
	private boolean isAvailable() {
		return getHazelcastOps().hazelcastInstance().getLifecycleService().isRunning();
	}
		
	private ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
		return delegate.schedule(task, trigger);
	}
	/**
	 * Schedule a one shot execution at a given time in future.
	 * @param task
	 * @param ctx
	 * @param startTime
	 * @return
	 */
	ScheduledFuture<?> scheduleAt(AbstractScheduledTask task, Date startTime) {
		return delegate.schedule(task, startTime);
	}
	/* (non-Javadoc)
	 * @see org.reactivetechnologies.ticker.scheduler.SchedulerManager#scheduleTask(org.reactivetechnologies.ticker.scheduler.AbstractScheduledTask)
	 */
	@Override
	public TaskContext scheduleTask(DistributedScheduledTask task)
	{
		TaskContext initialCtx = task.newTaskContext();
		task.setTaskKey(initialCtx);
		DelegatingCronTrigger cronTrigg = new DelegatingCronTrigger(task.cronExpression(), getClusterClock().getZone());
		task.setScheduler(this);
		task.setTrigger(cronTrigg);
		task.setContextMap(getHazelcastOps());
		task.publisher = pub;
		ScheduledFuture<?> future = schedule(task, cronTrigg);
		task.setCancellable(future);
		registry.put(initialCtx.getKeyParam(), task);
		return initialCtx;
	}
		
	/* (non-Javadoc)
	 * @see org.reactivetechnologies.ticker.scheduler.SchedulerManager#cancelTask(org.reactivetechnologies.ticker.scheduler.TaskContext, boolean)
	 */
	@Override
	public boolean cancelTask(TaskContext taskId, boolean cancelSpawnedTasks)
	{
		if(registry.containsKey(taskId.getKeyParam()))
		{
			cancelTask(registry.remove(taskId.getKeyParam()), cancelSpawnedTasks);
		}
		return false;
	}
	private void cancelTask(AbstractScheduledTask task, boolean cancelSpawnedTasks)
	{
		if(cancelSpawnedTasks && task instanceof DistributedScheduledTask)
			((DistributedScheduledTask) task).cancelSpawned();
		else
			task.cancel();
	}
	/* (non-Javadoc)
	 * @see org.reactivetechnologies.ticker.scheduler.SchedulerManager#destroy()
	 */
	@Override
	@PreDestroy
	public void destroy()
	{
		log.info("Destroying all schedulers registered ..");
		for(AbstractScheduledTask task : registry.values())
		{
			cancelTask(task, true);
		}
	}

	public long getClockOffset() {
		return clockOffset;
	}

}
