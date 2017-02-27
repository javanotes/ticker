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
package org.reactivetechnologies.ticker.messaging.base.ringbuff;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
/**
 * A collection of {@linkplain Disruptor LMAX disruptors}.
 * @author esutdal
 *
 */
class Disruptors {

	private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
	private final Map<String, Disruptor<DataWrapperEvent>> disruptors = new HashMap<>();
	public int size()
	{
		rwlock.readLock().lock();
		try
		{
			return disruptors.size();
			
		}
		finally{
			rwlock.readLock().unlock();
		}
		
	}
	@Override
	public String toString() {
		return "[" + disruptors + "]";
	}
	private static final Logger log = LoggerFactory.getLogger(Disruptors.class);
	public void register(String name,  int ringBufferSize, long pollTime, TimeUnit pollTimeUnit)
	{
		log.info("["+name+"] Initiating ring buffer with size "+ringBufferSize);
		Disruptor<DataWrapperEvent> disruptor = new Disruptor<>(DataWrapperEvent.EVENT_FACTORY, ringBufferSize, new ThreadFactory() {
			private int n=0;
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, name+"-disruptor-"+(n++));
				t.setDaemon(true);
				return t;
			}
		}, ProducerType.MULTI, new TimeoutBlockingWaitStrategy(pollTime, pollTimeUnit));
		rwlock.writeLock().lock();
		try
		{
			disruptors.put(name, disruptor);
			
		}
		finally{
			rwlock.writeLock().unlock();
		}
	}
	/**
	 * Register with a default ringbuffer size of 2^16 and polling unit in millis.
	 * @param name
	 * @param pollTime
	 */
	public void register(String name,  long pollTime)
	{
		register(name, (int) Math.pow(2, 16), pollTime);
	}
	/**
	 * Register with a provided ringbuffer size and polling unit in millis.
	 * @param name
	 * @param ringBufferSize
	 * @param pollTime
	 */
	public void register(String name,  int ringBufferSize, long pollTime)
	{
		register(name, ringBufferSize, pollTime, TimeUnit.MILLISECONDS);
	}
	
	public Disruptor<DataWrapperEvent> get(String name)
	{
		Assert.isTrue(disruptors.containsKey(name), "Disruptor not found for name- "+name);
		rwlock.readLock().lock();
		try
		{
			return disruptors.get(name);
			
		}
		finally{
			rwlock.readLock().unlock();
		}
	}
	/**
	 * 
	 * @param name
	 */
	public void start(String name)
	{
		rwlock.readLock().lock();
		try
		{
			if(name != null)
			{
				if(disruptors.containsKey(name))
					disruptors.get(name).start();
			}
			else
			{
				for(Disruptor<DataWrapperEvent> d : disruptors.values())
					d.start();
			}
			
		}
		finally{
			rwlock.readLock().unlock();
		}
	}
	public void stop(String name)
	{
		rwlock.readLock().lock();
		try
		{
			if(name != null)
			{
				if(disruptors.containsKey(name)){
					disruptors.get(name).shutdown();
				}
			}
			else
			{
				for(Disruptor<DataWrapperEvent> d : disruptors.values())
					d.shutdown();
			}
			
		}
		finally{
			rwlock.readLock().unlock();
		}
	}
	
}
