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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.reactivetechnologies.ticker.datagrid.HazelcastOperations;
import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.AbstractQueueContainer;
import org.reactivetechnologies.ticker.messaging.base.QueueListener;
import org.reactivetechnologies.ticker.messaging.data.DataWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.annotation.Value;

import com.lmax.disruptor.RingBuffer;
/**
 * @deprecated Experimental. The main challenge is the bounded nature of a ring buffer.
 * @author esutdal
 *
 */
public class RingBufferedQueueContainer extends AbstractQueueContainer {

	static final Logger log = LoggerFactory.getLogger(RingBufferedQueueContainer.class);
	private final Disruptors buffers;

	@Value("${container.ringbuff.threads:0}")
	private int fjWorkers;
	@Value("${container.ringbuff.size:0}")
	private int ringBuffSize;
	private ForkJoinPool threadPool;
	@Value("${container.clear_all_pending:false}")
	private boolean clearAllPendingEntries;
	
	/**
	 * 
	 * @param hazelWrap
	 */
	public RingBufferedQueueContainer(HazelcastOperations hazelWrap) {
		super(hazelWrap);
		buffers = new Disruptors();
	}

	@Override
	public long getPollInterval() {
		return pollInterval;
	}

	private int coreThreads;
	@PostConstruct
	void init()
	{
		coreThreads =  fjWorkers <= 0 ? Runtime.getRuntime().availableProcessors() : fjWorkers;
		threadPool = newFJPool(coreThreads, "ContainerWorkerPool");
	}
	@Override
	public void setPollInterval(long pollInterval) {
		this.pollInterval = pollInterval;
	}

	@Value("${container.ringbuff.poll_interval_millis:1000}")
	private long pollInterval;

	@PreDestroy
	@Override
	public void destroy() 
	{
		unregisterLocalEntryListeners();
		buffers.stop(null);
		threadPool.shutdown();
		try {
			threadPool.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			
		}
	}

	public boolean wasStarted()
	{
		return started.get();
	}
	private final AtomicBoolean started = new AtomicBoolean();
	@Override
	public void run() 
	{
		if(started.compareAndSet(false, true))
		{
			run0();
		}
		
	}
	
	private void run0()
	{
		log.info("Starting ring buffers, count #"+buffers.size());
		//start all ringbuffers
		buffers.start(null);
		log.info("Starting consumer workers on fork-join pool with core threads .."+coreThreads);
		startConsumerWorkers();
		registerLocalEntryListeners();
		log.info("Queue container started successfully");
		if (log.isDebugEnabled()) {
			log.debug("" + buffers);
		}
	}

	private void startConsumerWorkers() {
		for(ListenerRegistration lreg : listeners.values())
		{
			execute(lreg.getListener());
			processPendingEntries(lreg.getListener().routing(), clearAllPendingEntries);
		}
		
	}

	private <T extends Data> RingBufferedQueueContainerAction<T> newContainerTask(QueueListener<T> task)
	{
		return new RingBufferedQueueContainerAction<T>(task, buffers.get(task.routing()).getRingBuffer(), hazelWrap);
	}
	private void execute(QueueListener<? extends Data> task)
	{
		try {
			task.init();
		} catch (Exception e) {
			throw new BeanInitializationException("Exception on consumer init for task "+task.identifier(), e);
		}
		AbstractDisruptorRecursiveAction<? extends Data> runnable = newContainerTask(task);
		threadPool.execute(runnable);
	}
	
	@Override
	protected <T extends Data> void doRegister(QueueListener<T> listener) 
	{
		if(ringBuffSize > 0)
			buffers.register(listener.routing(), ringBuffSize, getPollInterval());
		else
			buffers.register(listener.routing(), getPollInterval());
	}

	@Override
	protected void onEntryAdded(DataWrapper consumable) 
	{
		
		RingBuffer<DataWrapperEvent> ringBuffer = buffers.get(consumable.data.getDestination()).getRingBuffer();
		long sequence = ringBuffer.next(); // Grab the next sequence
		try {
			DataWrapperEvent event = ringBuffer.get(sequence); // Get the entry in
			event.setEvent(consumable); // Fill with data
		} finally {
			ringBuffer.publish(sequence);
			if (log.isDebugEnabled()) {
				log.debug("Publishing to ring buffer sequence: " + sequence);
				log.debug("Data => " + consumable.data);
			}
		}

	}
	
	private static ForkJoinPool newFJPool(int coreThreads, String name)
	{
		return new ForkJoinPool(coreThreads, new ForkJoinWorkerThreadFactory() {
		      
		      private int containerThreadCount = 0;

			@Override
		      public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
		        ForkJoinWorkerThread t = new ForkJoinWorkerThread(pool){
		          
		        };
		        t.setName(name+".Worker."+(containerThreadCount ++));
		        return t;
		      }
		    }, new UncaughtExceptionHandler() {
		      
		      @Override
		      public void uncaughtException(Thread t, Throwable e) {
		        log.error("-- Uncaught exception in fork join pool --", e);
		        
		      }
		    }, true);
	}

}
