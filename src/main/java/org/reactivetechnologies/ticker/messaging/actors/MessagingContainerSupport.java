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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.reactivetechnologies.ticker.datagrid.AbstractMigratedPartitionListener;
import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.QueueListener;
import org.reactivetechnologies.ticker.messaging.dto.Consumable;
import org.reactivetechnologies.ticker.messaging.dto.__EntryRequest;
import org.reactivetechnologies.ticker.messaging.dto.__RegistrationRequest;
import org.reactivetechnologies.ticker.messaging.dto.__RunRequest;
import org.reactivetechnologies.ticker.messaging.dto.__StopRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.hazelcast.core.IMap;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import scala.concurrent.duration.FiniteDuration;
/**
 * A supporting class for interacting with the underlying {@linkplain QueueContainerActor}.
 * @author esutdal
 *
 */
public class MessagingContainerSupport extends AbstractMigratedPartitionListener {

	private static final Logger log = LoggerFactory.getLogger(MessagingContainerSupport.class);
	@Autowired
	private ActorSystem actors;
	
	@Autowired
	@Qualifier("containerActor")
	private ActorRef containerActor;
	
	@Override
	public String[] processingMaps() {
		String[] _arr = new String[queueMaps.size()];
		return queueMaps.toArray(_arr);
	}
	public void addProcessingMap(String map)
	{
		queueMaps.add(map);
	}
	private final List<String> queueMaps = Collections.synchronizedList(new ArrayList<>());
	@Override
	public void onMigratedKeySet(IMap<Object, Object> map, Set<Object> migratedKeySet) {
		for(Object key : migratedKeySet)
		{
			Consumable c = new Consumable((Data) map.get(key), false, key.toString());
			containerActor.tell(new __EntryRequest(c), ActorRef.noSender());
			
			log.info("Submitted migrated entry for key -> "+key);
		}
			
	}
	
	/**
	 * Register a new {@linkplain QueueListener} to the container.
	 * @param listener
	 */
	public <T extends Data> void registerListener(QueueListener<T> listener)
	{
		Inbox in = Inbox.create(actors);
		in.send(containerActor, new __RegistrationRequest<>(listener));
		try {
			in.receive(FiniteDuration.apply(10, TimeUnit.SECONDS));
		} catch (TimeoutException e) {
			throw new BeanCreationException("Operation failed", e);
		}
	}
	/**
	 * Start container if not already started.
	 */
	public void start()
	{
		containerActor.tell(new __RunRequest(), actors.guardian());
	}
	/**
	 * Gracefully stop container if started, awaiting for the given amount of time.
	 * If not successful within that time, then force stop. <p>Ideally, stopping the
	 * container need not be handled externally. It will try to recover from failures, however,
	 * will stop automatically on irrecoverable scenarios (as per Actor pattern).
	 * @param timeout
	 * @param unit
	 */
	public void stop(long timeout, TimeUnit unit)
	{
		Inbox in = Inbox.create(actors);
		in.send(containerActor, new __StopRequest(timeout, unit));
		boolean stopped = false;
		try {
			stopped = (boolean) in.receive(FiniteDuration.apply(10, TimeUnit.SECONDS));
		} catch (TimeoutException e) {
			
		}
		if(!stopped)
			actors.stop(containerActor);
	}
	
}
