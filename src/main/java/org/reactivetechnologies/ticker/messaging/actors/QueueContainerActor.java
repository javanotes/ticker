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

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivetechnologies.ticker.datagrid.HazelcastOperations;
import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.QueueContainer;
import org.reactivetechnologies.ticker.messaging.base.QueueListener;
import org.reactivetechnologies.ticker.messaging.dto.__RegistrationRequest;
import org.reactivetechnologies.ticker.messaging.dto.__RunRequest;
import org.reactivetechnologies.ticker.messaging.dto.__StopRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstanceNotActiveException;

import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategy.Directive;
import akka.actor.UntypedActor;
import akka.japi.Function;
import scala.concurrent.duration.Duration;

class QueueContainerActor extends UntypedActor implements QueueContainer {

	private static final Logger log = LoggerFactory.getLogger(QueueContainerActor.class);
	MessagingContainerSupport migrationListener;
	private final HazelcastOperations hazelWrap;
	private static SupervisorStrategy strategy = new OneForOneStrategy(10, Duration.apply(1, TimeUnit.MINUTES),
			new Function<Throwable, SupervisorStrategy.Directive>() {

				@Override
				public Directive apply(Throwable t) throws Exception {
					log.error("--Queue container caught error--", t);
					if(t instanceof HazelcastInstanceNotActiveException)
					{
						log.error("Stopping container actor hieararchy");
						return SupervisorStrategy.stop();
					}
					
					return SupervisorStrategy.restart();
				}
	});
	@Override
	public SupervisorStrategy supervisorStrategy()
	{
		return strategy;
	}
	private boolean clearAllPendingEntries, removeImmediate;
	/**
	 * 
	 * @param hazelWrap
	 */
	QueueContainerActor(HazelcastOperations hazelWrap) {
		this.hazelWrap = hazelWrap;
		log.debug("New instance @"+hashCode());
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public void onReceive(Object message) throws Throwable {
		if(message instanceof __RegistrationRequest)
		{
			register(((__RegistrationRequest<Data>) message).listener);
			getSender().tell(Boolean.TRUE, getSelf());
		}
		else if(message instanceof __RunRequest)
		{
			run();
			getSender().tell(Boolean.TRUE, getSelf());
		}
		else if(message instanceof __StopRequest)
		{
			awaitingStop = true;
			destroy();
			synchronized (stopWaitMutex) {
				if(awaitingStop)
				{
					stopWaitMutex.wait(((__StopRequest) message).unit.toMillis(((__StopRequest) message).timeout));
				}
			}
			getSender().tell(!awaitingStop, getSelf());
		}
		else
			unhandled(message);
	}

	private final Object stopWaitMutex = new Object();
	private volatile boolean awaitingStop;

	@Override
	public void destroy() {
		if(wasRun.compareAndSet(true, false)) 
			getContext().stop(getSelf());
	}
	
	@Override
	public void postStop() {
		synchronized (stopWaitMutex) {
			if(awaitingStop)
			{
				awaitingStop = false;
				stopWaitMutex.notify();
			}
		}
	}

	private AtomicBoolean wasRun = new AtomicBoolean();
	@Override
	public void run() {
		if (wasRun.compareAndSet(false, true)) 
		{
			for (Entry<String, Object> e : consumerActors.entrySet()) {
				if (e.getValue() instanceof Props) 
				{
					migrationListener.addProcessingMap(e.getKey());
					consumerActors.put(e.getKey(), getContext().actorOf((Props) e.getValue()));
					log.info("Actor started for queue " + e.getKey());
				}
			} 
		}
		
	}

	private final ConcurrentMap<String, Object> consumerActors = new ConcurrentHashMap<>();
	private final Object listenerMapValue = new Object();
	
	private <T extends Data> void registerIfNot(QueueListener<T> listener)
	{
		boolean notRegistered = false;
		if(!consumerActors.containsKey(listener.routing()))
		{
			notRegistered = consumerActors.putIfAbsent(listener.routing(), listenerMapValue) != null;
		}
		
		if(!notRegistered)
			register0(listener);
	}
	private<T extends Data> void register0(QueueListener<T> listener) {
		Props listenerProp = ConsumerSupervisorActor.newProps(listener, hazelWrap.hazelcastInstance(), isClearAllPendingEntries(), isRemoveImmediate());
		consumerActors.replace(listener.routing(), listenerMapValue, listenerProp);
	}


	@Override
	public <T extends Data> void register(QueueListener<T> listener) {
		//there should be only one listener per queue, per instance. Or else there will be multiple Hazelcast
		//entry listeners registered for the same IMap on the same instance.

		registerIfNot(listener);
	}

	@Override
	public long getPollInterval() {
		return -1;
	}

	@Override
	public void setPollInterval(long pollInterval) {
		// ignore
	}


	public boolean isClearAllPendingEntries() {
		return clearAllPendingEntries;
	}


	public void setClearAllPendingEntries(boolean clearAllPendingEntries) {
		this.clearAllPendingEntries = clearAllPendingEntries;
	}


	public boolean isRemoveImmediate() {
		return removeImmediate;
	}


	public void setRemoveImmediate(boolean removeImmediate) {
		this.removeImmediate = removeImmediate;
	}

}
