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
package org.reactivetechnologies.ticker.messaging.actors;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.MessageProcessingException;
import org.reactivetechnologies.ticker.messaging.base.DeadLetterHandler;
import org.reactivetechnologies.ticker.messaging.base.QueueListener;
import org.reactivetechnologies.ticker.messaging.data.DataWrapper;
import org.reactivetechnologies.ticker.messaging.dto.__CommitRequest;
import org.reactivetechnologies.ticker.messaging.dto.__DeadLetterRequest;
import org.reactivetechnologies.ticker.messaging.dto.__EntryRequest;
import org.reactivetechnologies.ticker.messaging.dto.__RetryRequest;
import org.reactivetechnologies.ticker.mqtt.MqttData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

import akka.actor.ActorRef;
import akka.actor.AllForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategy.Directive;
import akka.actor.UntypedActor;
import akka.japi.Function;
import scala.concurrent.duration.Duration;

/**
 * The supervisor actor for a particular queue listener.
 * @author esutdal
 *
 * @param <T>
 */
class ConsumerSupervisorActor<T extends Data> extends UntypedActor implements EntryAddedListener<Serializable, T>,EntryUpdatedListener<Serializable, T> {

	private static final Logger log = LoggerFactory.getLogger(ConsumerSupervisorActor.class);
	private ActorRef workerPool;
	private boolean clearAll, removeImmediate, checkExclusiveAccess;
	
	private final QueueListener<T> listener;
	private final HazelcastInstance hazelcast;

	private static SupervisorStrategy strategy = new AllForOneStrategy(10, Duration.apply(1, TimeUnit.MINUTES),
			new Function<Throwable, SupervisorStrategy.Directive>() {

				@Override
				public Directive apply(Throwable t) throws Exception {
					
					if(t instanceof MessageProcessingException)
					{
						log.error("--Message consume caught error--", t);
						return SupervisorStrategy.resume();
					}
					
					//let it get handled by QueueContainerActor
					return SupervisorStrategy.escalate();
				}
	});
	/**
	 * Private constructor
	 * @param l
	 * @param hz
	 */
	private ConsumerSupervisorActor(QueueListener<T> l, HazelcastInstance hz, boolean clearAll, boolean removeImmediate, boolean checkExclusiveAccess) {
		this.listener = l;
		this.hazelcast = hz;
		this.clearAll = clearAll;
		this.removeImmediate = removeImmediate;
		this.checkExclusiveAccess = checkExclusiveAccess;
		
		this.queueMap = hazelcast.getMap(listener.routing());
		
		//akka.routing.RoundRobinPool
		//akka.routing.SmallestMailboxPool;
		//akka.routing.BalancingPool;
		//configure the router pool?? 
		workerPool = getContext().actorOf(new akka.routing.BalancingPool(listener.parallelism()).withSupervisorStrategy(strategy)
				.props(Props.create(ConsumerWorkerActor.class, listener, this)));

		log.debug("New instance @" + hashCode());
	}

	private final IMap<Serializable, T> queueMap;
	
	/**
	 * Record as a dead letter. A {@linkplain DeadLetterHandler} will have the strategy to reject
	 * or retry message processing.
	 * @param msg
	 */
	private void recordDeadLetter(DataWrapper msg) {
		getContext().parent().tell(new __DeadLetterRequest(msg), getSelf());
	}

	private volatile String listnrRegId;

	private void registerLocalEntryListener()
	{
		processPendingEntries();
		addEntryListener();
	}
	@Override
	public void preStart() {
		listener.init();
		registerLocalEntryListener();
	}
	@Override
	public void postRestart(Throwable t)
	{
		registerLocalEntryListener();
	}
	/**
	 * Process the pending entries.
	 */
	private void processPendingEntries() 
	{
		T entry;
		EntryEvent<Serializable, T> event;
		for(Object key : clearAll ? queueMap.keySet() : queueMap.localKeySet())
		{
			entry = queueMap.get(key);
			event = new EntryEvent<Serializable, T>(listener.routing() + "-listener",
					hazelcast.getCluster().getLocalMember(), EntryEventType.MERGED.getType(), (Serializable) key,
					entry);
			entryAdded(event);
		}
		log.debug("Submitted pending entries with clearAll?"+clearAll);
	}

	private void removeEntryListener()
	{
		if (isHazelcastActive()) {
			queueMap.removeEntryListener(listnrRegId);
		}
	}
	private void addEntryListener()
	{
		if (isHazelcastActive()) {
			listnrRegId = queueMap.addLocalEntryListener(this);
		}
	}
	@Override
	public void postStop() {
		removeEntryListener();
		listener.destroy();
	}
	@Override
	public void preRestart(Throwable t, scala.Option<Object> msg)
	{
		removeEntryListener();
	}

	@Override
	public void onReceive(Object msg) throws Throwable {
		if(msg instanceof DataWrapper)
		{
			endTransaction((DataWrapper) msg);
		}
		else if(msg instanceof __EntryRequest)
		{
			DataWrapper consume = ((__EntryRequest) msg).consume;
			delegateExclusively(consume);
			
		}
		else if(msg instanceof __RetryRequest)
		{
			DataWrapper consume = ((__RetryRequest) msg).consume;
			delegateToWorker(consume);
			
		}
		else if(msg instanceof __CommitRequest)
		{
			DataWrapper consume = ((__CommitRequest) msg).consume;
			commitDelivery(consume, consume.isRemoveImmediate());
			
		}
		else
			unhandled(msg);
	}
	/**
	 * Complete the current consume by removing the entry from Hazelcast. This method can be invoked
	 * from the worker in the same thread as well (bypassing actor flow).
	 * @param msg
	 */
	final void endTransaction(DataWrapper msg) {
		
		if(msg.commit)
			commitDelivery(msg, msg.isRemoveImmediate());
		else
			recordDeadLetter(msg);
		
		
		log.debug("end transaction..");
	}

	public static <E extends Data> Props newProps(QueueListener<E> listener, HazelcastInstance hazel, boolean clearAll, boolean removeImmediate, boolean checkExclusiveAccess) {
		return Props.create(ConsumerSupervisorActor.class, listener, hazel, clearAll, removeImmediate, checkExclusiveAccess);
	}

	boolean isHazelcastActive()
	{
		return hazelcast.getLifecycleService().isRunning();
	}
	/**
	 * Commit message consumption by removing the entry from Hazelcast map.
	 * @param msg
	 * @param immediate 
	 */
	private void commitDelivery(DataWrapper msg, boolean immediate) {
		if(msg.data instanceof MqttData)
			notifyMqttAck((MqttData) msg.data);
		
		removeEntry(msg.key, immediate);
	}
	
	private void notifyMqttAck(MqttData data) {
				
	}
	private void removeEntry(Serializable key, boolean immediate)
	{
		if(immediate)
			queueMap.remove(key);
		else
			queueMap.removeAsync(key);
		log.debug("Commit..");
	}

	@Override
	public void entryAdded(EntryEvent<Serializable, T> event) {
		if (log.isDebugEnabled()) {
			log.debug("New entry received:: " + event);
		}
		onEntryEvent(event.getValue(), event.getKey());
	}
	
	private void onEntryEvent(T val, Serializable key)
	{
		DataWrapper consume = new DataWrapper(val, false, key);
		delegateExclusively(consume);
	}
	
	private void delegateExclusively(DataWrapper consume)
	{
		if (hasExclusiveAccess(consume)) {
			delegateToWorker(consume);
		}
	}
	private boolean hasExclusiveAccess(DataWrapper consumeMessage)
	{
		if (checkExclusiveAccess) 
		{
			queueMap.lock(consumeMessage.key);
			try {
				if (consumeMessage.data.getProcessState() == Data.STATE_OPEN) {
					consumeMessage.data.setProcessState(Data.STATE_LOCKED);
					return true;
				}
			} finally {
				queueMap.unlock(consumeMessage.key);
			}
			return false;
		}
		
		return true;
	}
	/**
	 * Delegate a consumer message to routee for execution.
	 * @param consumeMessage
	 */
	private void delegateToWorker(DataWrapper consumeMessage) {
		consumeMessage.setRemoveImmediate(removeImmediate);
		workerPool.tell(consumeMessage, getSelf());
	}
	public boolean isCheckExclusiveAccess() {
		return checkExclusiveAccess;
	}
	public void setCheckExclusiveAccess(boolean checkExclusiveAccess) {
		this.checkExclusiveAccess = checkExclusiveAccess;
	}
	@Override
	public void entryUpdated(EntryEvent<Serializable, T> event) {
		log.debug("Modified entry received:: "+event);
		onEntryEvent(event.getValue(), event.getKey());
	}
	
}
