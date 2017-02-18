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
import org.reactivetechnologies.ticker.messaging.base.QueueListener;
import org.reactivetechnologies.ticker.messaging.dto.Consumable;
import org.reactivetechnologies.ticker.messaging.dto.__EntryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;

import akka.actor.ActorRef;
import akka.actor.AllForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategy.Directive;
import akka.actor.UntypedActor;
import akka.japi.Function;
import akka.routing.BalancingPool;
//import akka.routing.RoundRobinPool;
//import akka.routing.SmallestMailboxPool;
import scala.concurrent.duration.Duration;

/**
 * The supervisor actor for a particular queue listener.
 * @author esutdal
 *
 * @param <T>
 */
class ConsumerSupervisorActor<T extends Data> extends UntypedActor implements EntryAddedListener<Serializable, T> {

	private static final Logger log = LoggerFactory.getLogger(ConsumerSupervisorActor.class);
	private ActorRef workerPool;
	private boolean clearAll, removeImmediate;
	
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
	private ConsumerSupervisorActor(QueueListener<T> l, HazelcastInstance hz, boolean clearAll, boolean removeImmediate) {
		this.listener = l;
		this.hazelcast = hz;
		this.clearAll = clearAll;
		this.removeImmediate = removeImmediate;
		
		this.queueMap = hazelcast.getMap(listener.routing());
		workerPool = getContext().actorOf(new BalancingPool(listener.parallelism()).withSupervisorStrategy(strategy)
				.props(Props.create(ConsumerWorkerActor.class, listener, this)));

		log.debug("New instance @" + hashCode());
	}

	private final IMap<Serializable, T> queueMap;
	
	/**
	 * Retry message delivery, if applicable.
	 * @param msg
	 */
	private void rollbackDelivery(Consumable msg) {
		getContext().parent().tell(msg, getSelf());
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

	private void processPendingEntries() 
	{
		T entry;
		Consumable item;
		for(Object key : clearAll ? queueMap.keySet() : queueMap.localKeySet())
		{
			entry = queueMap.get(key);
			item = new Consumable(entry, false, key.toString());
			delegateToWorker(item);
		}
		log.info("Submitted pending entries with clearAll?"+clearAll);
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
		if(msg instanceof Consumable)
		{
			endTransaction((Consumable) msg);
		}
		else if(msg instanceof __EntryRequest)
		{
			delegateToWorker(((__EntryRequest) msg).consume);
		}
		else
			unhandled(msg);
	}
	/**
	 * Complete the current consume by removing the entry from Hazelcast.
	 * @param msg
	 */
	void endTransaction(Consumable msg) {
		if(msg.commit)
			commitDelivery(msg, msg.isRemoveImmediate());
		else
			rollbackDelivery(msg);
		
		log.debug("end transaction..");
	}

	public static <E extends Data> Props newProps(QueueListener<E> listener, HazelcastInstance hazel, boolean clearAll, boolean removeImmediate) {
		return Props.create(ConsumerSupervisorActor.class, listener, hazel, clearAll, removeImmediate);
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
	private void commitDelivery(Consumable msg, boolean immediate) {
		if(immediate)
			queueMap.remove(msg.key);
		else
			queueMap.removeAsync(msg.key);
		log.debug("Commit..");
	}

	@Override
	public void entryAdded(EntryEvent<Serializable, T> event) {
		log.debug("New entry received:: "+event);
		delegateToWorker(new Consumable(event.getValue(), false, event.getKey().toString()));
	}
	/**
	 * Delegate a consumer message to routee for execution.
	 * @param consumeMessage
	 */
	private void delegateToWorker(Consumable consumeMessage) {
		consumeMessage.setRemoveImmediate(removeImmediate);
		workerPool.tell(consumeMessage, getSelf());
	}
	
}
