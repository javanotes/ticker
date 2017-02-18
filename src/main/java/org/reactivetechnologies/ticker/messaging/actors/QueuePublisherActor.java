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

import org.reactivetechnologies.ticker.datagrid.HazelcastOperations;
import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.ItemPartKeyGenerator;
import org.reactivetechnologies.ticker.messaging.base.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;

import akka.actor.UntypedActor;

class QueuePublisherActor extends UntypedActor implements Publisher {
	
	private static final Logger log = LoggerFactory.getLogger(QueuePublisherActor.class);
	
	QueuePublisherActor(HazelcastOperations hazelWrap) {
		this.hazelWrap = hazelWrap;
		log.debug("New instance @"+hashCode());
	}

	private IMap<Long, Data> getMap(Data d)
	{
		Assert.isTrue(StringUtils.hasText(d.getDestination()), "queue name not provided in Data");
		return hazelWrap.getMap(d.getDestination());
	}
	private final HazelcastOperations hazelWrap;
	private ItemPartKeyGenerator keyGen;
	@Override
	public <E extends Data> boolean offer(E item) {
		getMap(item).set(keyGen.getNext(item.getDestination()), item);
		return true;
	}

	@Override
	public <E extends Data> ICompletableFuture<Void> ingest(E item) {
		ICompletableFuture<Void> ret = getMap(item).setAsync(keyGen.getNext(item.getDestination()), item);
		return ret;
	}

	@Override
	public void onReceive(Object message) throws Throwable {
		if(message instanceof Data)
		{
			if(((Data) message).isAddAsync())
				ingest((Data) message);
			else{
				offer((Data)message);
				getSender().tell(Boolean.TRUE, getSelf());
			}
		}
		else
			unhandled(message);

	}

	public ItemPartKeyGenerator getKeyGen() {
		return keyGen;
	}

	public void setKeyGen(ItemPartKeyGenerator keyGen) {
		this.keyGen = keyGen;
	}

}
