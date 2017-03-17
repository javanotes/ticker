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
package org.reactivetechnologies.ticker.datagrid;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.map.listener.MapListener;
/**
 * Factory bean for getting instances of {@linkplain HazelcastOperations}. The underlying {@linkplain HazelcastInstance}
 * will be lazily instantiated.
 * @author esutdal
 *
 */
public class HazelcastOperationsFactoryBean implements FactoryBean<HazelcastOperations> {

	@Value("${instance.id:}")
	private String hzInstanceId;
	@Value("${instance.eagerJoin.enable:true}")
	private boolean hotStart;
	public String getHzInstanceId() {
		return hzInstanceId;
	}

	@PostConstruct
	private void init()
	{
		if(hotStart)
		{
			startWrapper();
		}
	}
	
	/**
	 * The proxy class that will be returned to clients on invoking {@linkplain HazelcastOperationsFactoryBean#getObject()}.
	 * @author esutdal
	 *
	 */
	private class HazelcastOperationsProxy implements HazelcastOperations 
	{
		@Override
		public Member thisMember() {
			return getWrapper().hazelcast.getCluster().getLocalMember();
		}

		@Override
		public void set(Object key, Object value, String map) {
			getMap(map).set(key, value);
		}

		@Override
		public Object removeNow(Serializable key, String map) {
			return getMap(map).remove(key);
		}

		@Override
		public boolean removeEntryListener(String id) {
			return getWrapper().hazelcast.removeDistributedObjectListener(id);
		}

		@Override
		public void remove(Object key, String map) {
			getMap(map).removeAsync(key);
		}

		@Override
		public Object put(Object key, Object value, String map) {
			return getMap(map).put(key, value);
		}

		@Override
		public ITopic<?> getTopic(String topic) {
			return getWrapper().hazelcast.getTopic(topic);
		}

		@Override
		public ISet<?> getSet(String set) {
			return getWrapper().hazelcast.getSet(set);
		}

		@Override
		public <K, V> IMap<K, V> getMap(String map) {
			return getWrapper().hazelcast.getMap(map);
		}

		@Override
		public ILock getClusterLock(String name) {
			return getWrapper().hazelcast.getLock(name);
		}

		@Override
		public IAtomicLong getAtomicLong(String key) {
			return getWrapper().hazelcast.getAtomicLong(key);
		}

		@Override
		public Object get(Object key, String map) {
			return getMap(map).get(key);
		}

		@Override
		public boolean contains(Serializable id, String imap) {
			return getMap(imap).containsKey(id);
		}

		@Override
		public String addLocalEntryListener(Serializable keyspace, MapListener listener) {
			return getWrapper().hazelcast.getMap(keyspace.toString()).addLocalEntryListener(listener);
		}
		@Override
		public void addMigrationListener(MigratedPartitionListener listener)
		{
			listener.setHazelcastInstance(getWrapper().hazelcast);
			getWrapper().getClusterListener().addObserver(listener);
		}

		@Override
		public HazelcastInstance hazelcastInstance() {
			return getWrapper().hazelcast;
		}

		@Override
		public <E> IQueue<E> getQueue(String queue) {
			return getWrapper().hazelcast.getQueue(queue);
		}

		@SuppressWarnings("unchecked")
		@Override
		public <V, K> List<Entry<K, V>> getLocalEntries(String map) {
			Set<K> keys = (Set<K>) getMap(map).localKeySet();
			List<Entry<K, V>> entries = new LinkedList<>();
			for(K key : keys)
			{
				V val = (V) get(key, map);
				entries.add(new Entry<K, V>() {

					@Override
					public K getKey() {
						return key;
					}

					@Override
					public V getValue() {
						return val;
					}

					@Override
					public V setValue(V arg0) {
						//immutable
						return val;
					}
				});
				
			}
			return entries;
		}
	}

	@Autowired private HazelcastInstanceWrapper wrapper;
	/**
	 * Package visible constructor.
	 */
	HazelcastOperationsFactoryBean() {
	}
	@Override
	public HazelcastOperations getObject() {
		
		startWrapper();
		return new HazelcastOperationsProxy();
	}

	private void startWrapper()
	{
		if(!getWrapper().isStarted())
			getWrapper().start(hzInstanceId);
	}
	@Override
	public Class<?> getObjectType() {
		return HazelcastOperations.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	HazelcastInstanceWrapper getWrapper() {
		return wrapper;
	}

	void setWrapper(HazelcastInstanceWrapper wrapper) {
		this.wrapper = wrapper;
	}
	
}
