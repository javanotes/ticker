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

import org.reactivetechnologies.ticker.datagrid.HazelcastOperations;
import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.QueueListener;
import org.reactivetechnologies.ticker.messaging.dto.Consumable;
import org.reactivetechnologies.ticker.messaging.dto.ConsumableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;

class RingBufferedQueueContainerAction<T extends Data> extends AbstractDisruptorRecursiveAction<T> {

	private static final Logger log = LoggerFactory.getLogger(RingBufferedQueueContainerAction.class);
	public RingBufferedQueueContainerAction(QueueListener<T> ql, RingBuffer<ConsumableEvent> ringBuffer, HazelcastOperations hazelWrap) {
		super(ql, ringBuffer, hazelWrap);
	}

	private RingBufferedQueueContainerAction(QueueListener<T> consumer, int i, RingBuffer<ConsumableEvent> ringBuffer,
			SequenceBarrier barrier, Sequence sequence) {
		super(consumer, i, ringBuffer, barrier, sequence);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public AbstractDisruptorRecursiveAction<T> copy() {
		RingBufferedQueueContainerAction<T> b = new RingBufferedQueueContainerAction<T>(consumer, 1, ringBuffer, barrier, sequence);
		b.setHazelWrap(getHazelWrap());
		return b;
	}
	private boolean isAllowedPartition(long seq, int part)
	{
		if(consumer.parallelism() > 1)
		{
			return seq % consumer.parallelism() == part;
		}
		return true;
	}
	@Override
	protected  Consumable fetchNextInRing(int partition) throws Exception
    {
		log.debug("PARTITION............"+partition);
        ConsumableEvent event = null;
        long currentSequence = sequence.get();
        long nextSequence = currentSequence+1;
        try
        {
        	final long availableSequence = barrier.waitFor(nextSequence);
        	
        	nextSequence = Math.min(nextSequence, availableSequence);
        	
        	if(!isAllowedPartition(nextSequence, partition))
        		return null;
        	
        	event = ringBuffer.get(nextSequence);

            sequence.compareAndSet(currentSequence, nextSequence);
        } 
        catch (AlertException | InterruptedException | com.lmax.disruptor.TimeoutException e) 
        {
        	handleRingFetchException(e);
		}
        catch (Throwable t)
        {
        	sequence.compareAndSet(currentSequence, nextSequence);
        	log.error("Sequence slot being incremented on error "+currentSequence, t);
        }
        
		return event != null ? event.getEvent() : null;
    }
	
	private void handleRingFetchException(Exception e)
	{
    	if(e instanceof InterruptedException)
    		Thread.currentThread().interrupt();
    	if(e instanceof TimeoutException)
    		fireOnTimeout(new java.util.concurrent.TimeoutException());
	}

}
