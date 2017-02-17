package org.reactivetechnologies.ticker.messaging.base.ringbuff;

import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeoutException;

import org.reactivetechnologies.ticker.datagrid.HazelcastOperations;
import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.QueueListener;
import org.reactivetechnologies.ticker.messaging.dto.Consumable;
import org.reactivetechnologies.ticker.messaging.dto.ConsumableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.Sequencer;
/**
 * The task class that works in a work-stealing thread pool.
 * @author esutdal
 *
 */
abstract class AbstractDisruptorRecursiveAction<T extends Data> extends RecursiveAction implements QueueContainerAction<T>
{
	private static final Logger log = LoggerFactory.getLogger(AbstractDisruptorRecursiveAction.class);
	protected final int concurrency;
	protected final QueueListener<T> consumer;
	protected final RingBuffer<ConsumableEvent> ringBuffer;
	protected final SequenceBarrier barrier;
	protected final Sequence sequence;
	private HazelcastOperations hazelWrap;
	
	private int partition = 0;
	/**
	 * Instantiates a new task with concurrency level as set in the consumer. This constructor is kept
	 * public to schedule the first shot of task from the container.
	 * @param <T>
	 * @param ql
	 */
	public AbstractDisruptorRecursiveAction(QueueListener<T> ql, RingBuffer<ConsumableEvent> ringBuffer, HazelcastOperations hazelWrap) {
		this(ql, ql.parallelism(), ringBuffer, ringBuffer.newBarrier(new Sequence[0]), new Sequence(Sequencer.INITIAL_CURSOR_VALUE));
		this.setHazelWrap(hazelWrap);
	}
	/**
	 * Fork new parallel tasks to be scheduled in a work stealing pool. This constructor will be invoked from within
	 * the {@linkplain RecursiveAction} compute, to fork new tasks.
	 * @param ql
	 * @param concurrency
	 */
	protected AbstractDisruptorRecursiveAction(QueueListener<T> ql, int concurrency, RingBuffer<ConsumableEvent> ringBuffer, SequenceBarrier barrier, Sequence sequence) {
		this.concurrency = concurrency;
		this.consumer = ql;
		this.ringBuffer = ringBuffer;
		this.barrier = barrier;
		this.sequence = sequence;
	}
	
	/**
	 * Fetch the next available item to be read in sequence from the ring buffer.
	 * @return
	 * @throws Exception
	 */
	protected  abstract Consumable fetchNextInRing(int partition) throws Exception;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5690149379909633509L;

	@Override
	protected final void compute() {
		if (concurrency > 1) 
		{
			forkTasks(concurrency);
		} 
		else 
		{
			process();
		}

	}
	/* (non-Javadoc)
	 * @see org.reactivetechnologies.ticker.messaging.base.ringbuff.QueueContainerRecursiveAction#copy()
	 */
	public abstract AbstractDisruptorRecursiveAction<T> copy();
	
	/**
	 * Fork parallel tasks based on the concurrency. 
	 * These tasks should be available for work-stealing via FJpool.
	 * @param parallelism
	 */
	protected void forkTasks(int parallelism)
	{
		AbstractDisruptorRecursiveAction<T> qt = null;
		for(int i=0; i<parallelism; i++)
        {
        	qt = copy();
        	qt.partition = parallelism == 1 ? this.partition : i;
        	qt.fork();
        }
		
	}
	
	@SuppressWarnings("unchecked")
	private void fireOnMessage(Consumable nextMessage)
	{
		try 
		{
			consumer.onMessage((T) nextMessage.data);
		}  
		catch(Exception e)
		{
			handleException((T) nextMessage.data, e);
			
		}
	}
	
	protected void fireOnTimeout(TimeoutException t) {
		//noop
	}
	
	/**
	 * 
	 * @param qr
	 * @param e
	 */
	protected void handleException(T qr, Exception e)
	{
		log.warn("---Exception while processing message--- "+qr);
		log.warn("", e);
	}
	
	/* (non-Javadoc)
	 * @see org.reactivetechnologies.ticker.messaging.base.ringbuff.QueueContainerRecursiveAction#process()
	 */
	@Override
	public void process() 
	{
		try 
		{
			Consumable nextMessage = fetchNextInRing(partition);
			if (nextMessage != null) {
				if (log.isDebugEnabled()) {
					log.debug(nextMessage + "");
				}
				fireOnMessage(nextMessage);
				commitDelivery(nextMessage);
			} 
			 
		}
		catch(Exception e)
		{
			e.printStackTrace();
			log.error("Internal error: Check stacktrace", e);
		}
		finally 
		{
			forkTasks(1);//fork next corresponding task
		}
	}
	private void commitDelivery(Consumable nextMessage) {
		hazelWrap.remove(nextMessage.key, nextMessage.data.getDestination());
	}
	public HazelcastOperations getHazelWrap() {
		return hazelWrap;
	}
	public void setHazelWrap(HazelcastOperations hazelWrap) {
		this.hazelWrap = hazelWrap;
	}
		
}
