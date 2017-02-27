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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.reactivetechnologies.ticker.datagrid.HazelcastOperations;
import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.Publisher;
import org.reactivetechnologies.ticker.messaging.base.QueueListener;
import org.reactivetechnologies.ticker.utils.ApplicationContextHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronSequenceGenerator;
import org.springframework.util.Assert;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;

/**
 * The base class to extend for creating distributed {@linkplain ScheduledTask} instances. 
 * This class will be passed a {@linkplain TaskContext} on each run.
 * @author esutdal
 * @see #scheduleTimeunit()
 * @see #scheduleLockExpiryMilis()
 *
 */
public abstract class DistributedScheduledTask extends AbstractScheduledTask {

	static final Logger log = LoggerFactory.getLogger(DistributedScheduledTask.class);
	
	protected final Set<ScheduledTask> spawnedTasks = Collections.synchronizedSet(new HashSet<>());
	private volatile TaskContext key;
	protected Publisher publisher;
	/**
	 * New context with the {@link #name()} as the identifier.
	 * @return
	 */
	public TaskContext newTaskContext()
	{
		return newTaskContext(name());
	}
	/**
	 * New context with the given paramKey as context identifier.
	 * @param paramKey
	 * @return
	 */
	public TaskContext newTaskContext(String paramKey)
	{
		TaskContext ctx = new TaskContext(paramKey);
		return ctx;
	}
	final void setTaskKey(TaskContext key) {
		this.key = key;
	}
	
	/**
	 * Cancel all spawned tasks before canceling itself.
	 * @return
	 */
	boolean cancelSpawned()
	{
		boolean done = false;
		if(!spawnedTasks.isEmpty())
		{
			for(ScheduledTask kr : spawnedTasks)
			{
				done &= kr.cancel();
			}
		}
		done &= cancel();
		return done;
	}
	/**
	 * A distributed task execution context map. Being distributed in nature, the setter/getter operations
	 * on it will keep the entries in a Hazelcast IMap with {@link #getKeyParam()} as the name. It is important to note, due to the distributed nature
	 * the entries set in the context will be available in other executions as well. <p>So for purely local context it is suggested to use the {@link #setTransient(Object, Object)}
	 * and {@link #getTransient(Object)} variants of the setter/getter. These entries will be passed on to a corresponding child task only.
	 * @author esutdal
	 *
	 */
	public final class TaskContext implements Serializable,Map<Serializable, Data> {
		/**
		 * Create a new instance with the same key.
		 * @return
		 */
		public TaskContext copy()
		{
			return new TaskContext(getKeyParam());
		}
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		/**
		 * 
		 * @param keyParam
		 */
		private TaskContext(String keyParam) {
			super();
			this.keyParam = keyParam;
		}

		private final String keyParam;

		/**
		 * Getter for keyParam. This will correspond to one unique {@linkplain DistributedScheduledTask}.
		 * @return The key for this context
		 */
		public String getKeyParam() {
			return keyParam;
		}
		/**
		 * Emit next Data for asynchronous processing in the cluster. The data will be submitted
		 * to a distributed processing queue as specified by {@linkplain Data#getDestination()}.
		 * @see {@linkplain QueueListener#onMessage(Data)}
		 * @param d
		 */
		public void emit(Data d)
		{
			doEmit(d);
		}
		private Map<Object, Object> transientMap = new LinkedHashMap<Object, Object>();
		public void setTransient(Object key, Object value)
		{
			transientMap.put(key, value);
		}
		public boolean containsKeyTransient(Object key)
		{
			return transientMap.containsKey(key);
		}
		public Object getTransient(Object key)
		{
			return transientMap.get(key);
		}
		public void clearTransient()
		{
			transientMap.clear();
		}
		
		@Override
		public void clear() {
			getContextMap(keyParam).clear();
			
		}
		@Override
		public boolean containsKey(Object arg0) {
			return getContextMap(keyParam).containsKey(arg0);
		}
		@Override
		public boolean containsValue(Object arg0) {
			return getContextMap(keyParam).containsValue(arg0);
		}
		@Override
		public Set<java.util.Map.Entry<Serializable, Data>> entrySet() {
			return getContextMap(keyParam).entrySet();
		}
		@Override
		public Data get(Object arg0) {
			return getContextMap(keyParam).get(arg0);
		}
		@Override
		public boolean isEmpty() {
			return getContextMap(keyParam).isEmpty();
		}
		@Override
		public Set<Serializable> keySet() {
			return getContextMap(keyParam).keySet();
		}
		@Override
		public Data put(Serializable arg0, Data arg1) {
			return getContextMap(keyParam).put(arg0, arg1);
		}
		@Override
		public void putAll(Map<? extends Serializable, ? extends Data> arg0) {
			getContextMap(keyParam).putAll(arg0);
		}
		@Override
		public Data remove(Object arg0) {
			return getContextMap(keyParam).remove(arg0);
		}
		@Override
		public int size() {
			return getContextMap(keyParam).size();
		}
		@Override
		public Collection<Data> values() {
			return getContextMap(keyParam).values();
		}
		public boolean isSpawnChildTask() {
			return spawnChildTask;
		}
		/**
		 * Set to true if a {@linkplain SpawnedScheduledTask} need to be spawned from the {@linkplain DistributedScheduledTask}. The child task should
		 * already be set with the parent.
		 * @param spawnChildTask
		 * @see {@linkplain DistributedScheduledTask#setChildTask(Object)}
		 */
		public void spawnChildTask(boolean spawnChildTask) {
			this.spawnChildTask = spawnChildTask;
		}
		private boolean spawnChildTask;

	}

	/**
	 * This method is invoked after every successful invocation of {@link #run(TaskContext)}. This method can be 
	 * used to schedule a new task. The scheduled task will be maintained as a child task of this instance.
	 * @param context
	 * @return
	 */
	private SpawnedScheduledTask spawnTask(TaskContext context)
	{
		if(context.isSpawnChildTask())
		{
			return childsThreadLocal.get();
		}
		return null;
	}
	/**
	 * A supplier of {@link SpawnedScheduledTask} tasks.
	 * @author esutdal
	 *
	 */
	private class SpawnedScheduledTaskFactory implements Supplier<SpawnedScheduledTask>
	{
		public SpawnedScheduledTaskFactory() {
			
		}
		private SpawnedScheduledTask singleton;

		@SuppressWarnings("unchecked")
		private SpawnedScheduledTask scanForChildTask() {
			Assert.notNull(spawnedTask, "No SpawnedScheduledTask set but spawnChildTask is true");
			SpawnedScheduledTask subTask = null;
			log.info("Creating new sub task instance..");
			if (spawnedTask instanceof Class) {
				subTask = (SpawnedScheduledTask) ApplicationContextHelper
						.scanForClassInstance((Class<? extends SpawnedScheduledTask>) spawnedTask, "");

				if (subTask == null)
					subTask = (SpawnedScheduledTask) ApplicationContextHelper
							.getInstance((Class<? extends SpawnedScheduledTask>) spawnedTask);
			} else if (spawnedTask instanceof String) {
				subTask = (SpawnedScheduledTask) ApplicationContextHelper.scanFromContext(spawnedTask.toString());

				if (subTask == null)
					subTask = (SpawnedScheduledTask) ApplicationContextHelper
							.scanForClassInstance(spawnedTask.toString());
			}
			if (subTask != null) {
				subTask.setScheduler(DistributedScheduledTask.this.getScheduler());
				subTask.setTrigger(DistributedScheduledTask.this.getTrigger());

				log.info("New child task spawned " + subTask.name());
			}

			return subTask;
		}

		@Override
		public SpawnedScheduledTask get() {
			//double checking is not really necessary here.
			if(singleton == null)
				singleton = scanForChildTask();
			
			return singleton;
		}

	}
	//TODO: consider using an eager pooling approach for performance improvement
	private ThreadLocal<SpawnedScheduledTask> childsThreadLocal = ThreadLocal.withInitial(new SpawnedScheduledTaskFactory());
	
	/**
	 * Set a child {@linkplain SpawnedScheduledTask} to be run as a subtask whenever this task is run. This will trigger
	 * the child task as a one shot task, AFTER each scheduled run of the parent task.<p> This task could be either declared as a Spring bean ( if autowired by type, then set the 
	 * class type for it, else set a string with the bean name)
	 * , or with a string depicting the fully qualified class name.
	 * @param spawnChildTask either a {@linkplain String} (Spring bean name / FQ class name) or {@linkplain Class} (Spring bean type)
	 * @see TaskContext#spawnedTask
	 */
	public void setChildTask(Object spawnChildTask) {
		this.spawnedTask = spawnChildTask;
	}
	private Object spawnedTask;
	
	private void doRun()
	{
		final TaskContext context = key.copy();
		doRun(context);
		doSpawn(context);
	}
	private void spawnContext(SpawnedScheduledTask spawnedTask, TaskContext context)
	{
		spawnedTask.setPassedContext(context);
		if (spawnedTask.cronExpression() != null) {
			CronSequenceGenerator cron = new CronSequenceGenerator(spawnedTask.cronExpression(),
					getTrigger().getTzone());
			getScheduler().scheduleAt(spawnedTask, cron.next(getTrigger().getLastExecutionTime()));
		} else{
			getScheduler().scheduleAt(spawnedTask,
					new Date(getTrigger().getLastExecutionTime().getTime() + spawnedTask.executeAfter().toMillis()));
		}
	}
	private void doSpawn(TaskContext context)
	{
		SpawnedScheduledTask spawnedTask = spawnTask(context);
		if (spawnedTask != null) 
		{
			spawnContext(spawnedTask, context);
			spawnedTasks.add(spawnedTask);
		}
		else
		{
			spawnedTasks.remove(this);
		}
	}
	
	private void doEmit(Data d)
	{
		if (d.isAddAsync()) {
			publisher.ingest(d);
		}
		else
			publisher.offer(d);
		log.info("Emitted data for reactive processing..");
	}
		
	@Override
	public void run() {
		try 
		{
			if (acquireLock()) {
				markLocked();
				doRun();
			}
			else
				log.info("Did not acquire this run distributed mutex");
		} 
		catch (HazelcastInstanceNotActiveException e) {
			log.error("Hazelcast unavailable. "
					+ (isInLockingState ? "Scheduler ran but is left in a locked state now" : "Scheduler was not run!"));
			log.debug("", e);
			if(isInLockingState)
			{
				//should be handled by Hazelcast.
				log.error("This is an unexpected scenario! The schedule has been locked, but probably Hazelcast was shutdown. This is "
						+ "an irrecoverable situation and is being left without any action being taken. Please check data consistency manually.");
			}
		}
		catch (Exception e) {
			log.error("Scheduler execution exception logged", e);
		}
	}
	private volatile boolean isInLockingState;
	private void markLocked() {
		setInLockingState(true);
	}
	static final byte[] VALUE = ".".getBytes(StandardCharsets.UTF_8);
	static final Integer KEY = 1;
	
	/**
	 * The lowest denomination of {@linkplain TimeUnit} till which uniqueness of job execution is guaranteed.
	 * Can be HOUR or MINUTE or SECOND. Should be implemented by subclasses. <p><b>Note:</b> Specifying a correct unit is crucial
	 * since a timestamp pattern based on this unit will be used to acquire a unique scheduler run, without using
	 * any distributed clock synchronizing technique.
	 * @return
	 */
	protected abstract TimeUnit scheduleTimeunit();
	/**
	 * Time in milliseconds, for which an acquired schedule will be locked exclusively. Implies, another instance which has a lagging system clock
	 * might have the possibility of rerunning a schedule. To avoid this, the value should be chosen with sufficient buffer for such time lag. <p>Default is 10 minutes.
	 * <b>Thus we are assuming that a maximum clock lag between any 2 instances in the cluster will not be more than 10 minutes.</b> 
	 * Override the value as required.
	 * @return
	 */
	protected long scheduleLockExpiryMilis()
	{
		return TimeUnit.MINUTES.toMillis(10);
	}
	protected String getTimestampKey()
	{
		Clock clock = getScheduler().getClusterClock();
		clock.setTimestamp(getTrigger().getNextExecutionTime().getTime());
		return clock.toTimestampString(scheduleTimeunit());
	}
	private volatile String timestampKey;
	private HazelcastOperations contextMap;
	/**
	 * Acquire cluster lock. This method is responsible for guaranteeing uniqueness in a scheduled task run
	 * across the cluster. Package private access.
	 * @return
	 */
	boolean acquireLock() 
	{
		timestampKey = getTimestampKey();
		log.debug("timestampKey- "+timestampKey);
		IMap<String, byte[]> map = getScheduler().getHazelcastOps().getMap(getClass().getName());
		map.lock(timestampKey);
		try {
			return map.putIfAbsent(timestampKey, VALUE, scheduleLockExpiryMilis(), TimeUnit.MILLISECONDS) == null;
		} finally {
			map.unlock(timestampKey);
		}
	}
	/**
	 * Whether this instance has an acquired lock.
	 * @return
	 */
	public boolean isInLockingState() {
		return isInLockingState;
	}
	private void setInLockingState(boolean isInLockingState) {
		this.isInLockingState = isInLockingState;
	}
	
	private IMap<Serializable, Data> getContextMap(String contextName) {
		return contextMap.getMap(contextName);
	}
	public void setContextMap(HazelcastOperations contextMap) {
		this.contextMap = contextMap;
	}
}
