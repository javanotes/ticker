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
package org.reactivetechnologies.ticker.messaging.base;

import java.io.Closeable;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.reactivetechnologies.ticker.datagrid.HazelcastOperations;
import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.ringbuff.RingBufferedQueueContainer;
import org.reactivetechnologies.ticker.messaging.dto.Consumable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
/**
 * @deprecated For use in {@linkplain RingBufferedQueueContainer}
 * @author esutdal
 *
 */
public abstract class AbstractQueueContainer implements QueueContainer, EntryAddedListener<Serializable, Data> {

	private final static Logger log = LoggerFactory.getLogger(AbstractQueueContainer.class);
	protected final HazelcastOperations hazelWrap;

	/**
	 * 
	 * @param hazelWrap
	 */
	public AbstractQueueContainer(HazelcastOperations hazelWrap) {
		this.hazelWrap = hazelWrap;
	}

	protected final ConcurrentMap<String, ListenerRegistration> listeners = new ConcurrentHashMap<>();
	public class ListenerRegistration implements Closeable
	{
		@Override
		public String toString() {
			return "[regnId=" + regnId + ", listening_on=" + listener.routing() + "]";
		}
		public String getRegnId() {
			return regnId;
		}
		public QueueListener<? extends Data> getListener() {
			return listener;
		}
		private String regnId;
		private QueueListener<? extends Data> listener;
		@Override
		public void close()  {
			hazelWrap.removeEntryListener(regnId);
			listener.destroy();
		}
		
	}
	
	protected void unregisterLocalEntryListeners()
	{
		for(ListenerRegistration lr : listeners.values())
		{
			lr.close();
		}
	}
	
	private <T extends Data> void registerIfNot(QueueListener<T> listener) {
		boolean notRegistered = false;
		if (!listeners.containsKey(listener.routing())) {
			notRegistered = listeners.putIfAbsent(listener.routing(), new ListenerRegistration()) != null;
		}

		if (!notRegistered){
			ListenerRegistration lreg = listeners.get(listener.routing());
			lreg.listener = listener;
			doRegister(listener);
			
		}
	}
	/**
	 * Add this container as a local entry listener for the {@linkplain IMap}s corresponding to the {@linkplain QueueListener#routing()}.
	 * @param listener
	 */
	protected <T extends Data> void registerLocalEntryListeners()
	{
		for(ListenerRegistration lreg : listeners.values())
		{
			lreg.regnId = hazelWrap.addLocalEntryListener(lreg.listener.routing(), this);
			log.info("Registered local entry listener => "+lreg);
		}
		
	}
	/**
	 * Register this unique {@linkplain QueueListener}. No duplicate checking need to be
	 * performed at this stage by implementing classes. This method is kept for additional 
	 * registration operations to be performed by subclasses.
	 * @param listener
	 */
	protected <T extends Data> void doRegister(QueueListener<T> listener){}
	
	@Override
	public <T extends Data> void register(QueueListener<T> listener) {
		// there should be only one listener per queue, per instance. Or else
		// there will be multiple Hazelcast
		// entry listeners registered for the same IMap on the same instance.

		registerIfNot(listener);

	}

	/**
	 * Process currently present map entries.
	 * @param mapName
	 * @param all true, if all entries to be processed
	 */
	protected <T extends Data> void processPendingEntries(String mapName, boolean all) {
		IMap<Serializable, T> imap = hazelWrap.getMap(mapName);
		T entry;
		Consumable item;
		for(Object key : all ? imap.keySet() : imap.localKeySet())
		{
			entry = imap.get(key);
			item = new Consumable(entry, false, key.toString());
			onEntryAdded(item);
		}
		log.info("Cleared "+(all ? "all " : "local ")+"pending entries for "+mapName);
		
	}
	@Override
	public void entryAdded(EntryEvent<Serializable, Data> event) {
		onEntryAdded(new Consumable(event.getValue(), false, event.getKey().toString()));
	}
	/**
	 * Callback on new item received. To be implemented by subclass. The callback will
	 * be active only on a successful return of {@link #doRegister(QueueListener)}.
	 * @param consumable
	 */
	protected abstract void onEntryAdded(Consumable consumable);
}
