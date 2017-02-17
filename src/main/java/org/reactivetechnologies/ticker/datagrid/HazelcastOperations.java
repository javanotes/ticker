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
 * In-memory datagrid (IMDG) operations that can be performed using a {@linkplain HazelcastInstance}.
 * @author esutdal
 *
 */
public interface HazelcastOperations {

	/**
	 * Get the underlying {@linkplain HazelcastInstance}.
	 * @return
	 */
	HazelcastInstance hazelcastInstance();	
	/**
	 * Asynchronous removal of a key from IMap.
	 * @param key
	 * @param map
	 */
	void remove(Object key, String map);

	/**
	   * Gets the Hazelcast IMap instance.
	   * @param map
	   * @return
	   */
	<K, V> IMap<K, V> getMap(String map);

	/**
	   * Map put operation.
	   * @param key
	   * @param value
	   * @param map
	   * @return
	   */
	Object put(Object key, Object value, String map);

	/**
	   * 
	   * @param key
	   * @param value
	   * @param map
	   */
	void set(Object key, Object value, String map);

	/**
	   * Get the value corresponding to the key from an {@linkplain IMap}.
	   * @param key
	   * @param map
	   * @return
	   */
	Object get(Object key, String map);

	
	
	/**
	 * Register a local add/update entry listener on a given {@linkplain IMap} by name. Only a single listener for a given {@linkplain MapListener} instance would be 
	 * registered. So subsequent invocation with the same instance would first remove any existing registration for that instance.
	 * @param keyspace map name
	 * @param listener callback 
	 * @return 
	 * @throws IllegalAccessException 
	 */
	String addLocalEntryListener(Serializable keyspace, MapListener listener);

	/**
	 * Removes the specified map entry listener. Returns silently if there is no such listener added before.
	 * @param id
	 * @return
	 */
	boolean removeEntryListener(String id);

	/**
		 * Synchronous removal of a key from IMap.
		 * @param key
		 * @param map
		 * @return
		 */
	Object removeNow(Serializable key, String map);

	/**
	   * Get a distributed cluster wide lock.
	   * @param name
	   * @return
	   */
	ILock getClusterLock(String name);

	/**
	   * Checks if an {@linkplain IMap} contains the given key.
	   * @param id
	   * @param imap
	   * @return
	   */
	boolean contains(Serializable id, String imap);

	/**
	   * Get topic instance with the given name.
	   * @param topic
	   * @return
	   */
	ITopic<?> getTopic(String topic);
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	IAtomicLong getAtomicLong(String key);

	/**
	 * 
	 * @param set
	 * @return
	 */
	ISet<?> getSet(String set);

	/**
	 * The running instance as {@linkplain Member}
	 * @return
	 */
	Member thisMember();
	/**
	 * Get Iqueue instance
	 * @param <E>
	 * @param queue
	 * @return
	 */
	<E> IQueue<E> getQueue(String queue);
	/**
	 * 
	 * @param listener
	 */
	void addMigrationListener(MigratedPartitionListener listener);

}