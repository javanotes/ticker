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

import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.QueueListener;
import org.reactivetechnologies.ticker.messaging.dto.Consumable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.UntypedActor;

final class ConsumerWorkerActor<E extends Data> extends UntypedActor {
	private final QueueListener<E> listener;
	private ConsumerSupervisorActor<E> supervisorInstance;
	
	private static final Logger log = LoggerFactory.getLogger(ConsumerWorkerActor.class);
	
	public ConsumerWorkerActor(QueueListener<E> listener, ConsumerSupervisorActor<E> supervisorInstance) {
		this.listener = listener;
		this.supervisorInstance = supervisorInstance;
		log.debug("New instance @"+hashCode());
	}
	@Override
	public void onReceive(Object msg) throws Throwable {
		if (msg instanceof Consumable) {
			executeDelivery((Consumable) msg);
		} else
			unhandled(msg);

	}
	//The router actor forwards messages onto its routees without changing the original sender. 
	//When a routee replies to a routed message, the reply will be sent to the original sender, not to the router actor.
	
	@SuppressWarnings("unchecked")
	private void executeDelivery(Consumable msg) throws Exception {
		boolean commit = false;
		try
		{
			listener.onMessage((E) msg.data);
			commit = true;
		}
		finally
		{
			if(msg.isRemoveImmediate())
				supervisorInstance.endTransaction(new Consumable(msg.data, commit, msg.key));
			else
				getSender().tell(new Consumable(msg.data, commit, msg.key), getSelf());
		}
		
	}

}