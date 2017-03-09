/* ============================================================================
*
* FILE: MapData.java
*
The MIT License (MIT)

Copyright (c) 2016 Sutanu Dalui

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*
* ============================================================================
*/
package org.reactivetechnologies.ticker.messaging.data;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.utils.ApplicationContextWrapper;
import org.springframework.util.Assert;

import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

/**
 * A {@linkplain Data} extension that encapsulates a {@linkplain Map} data structure. The underlying implementation is a 'Hazelcast friendly' sorted map data structure
 * that can be used as an {@linkplain IMap} entry (key/value). The local entries in Hazlecast {@linkplain IMap} are not sorted. This data structure can thus be used 
 * to have locally sorted {@linkplain Map} entries.<p>Note: This class can be extended for creating Apache Cassandra like data models with composite keys of <i>partitioning</i> (used for distribution)
 * and <i>clustering</i> (used for sorting).
 * @param <K>
 * @param <V>
 */
public class MapData<K extends DataComparable<?>, V extends DataSerializable> extends Data implements Map<K, V> {

	private SortedMap<K, V> sortedMap = new TreeMap<>();
	/**
	 * 
	 */
	private static final long serialVersionUID = 6294748301538391817L;

	/**
	 * Default constructor.
	 */
	public MapData() {
		super();
	}

	/**
	 * New message with a {@linkplain Map} payload.
	 * 
	 * @param s
	 */
	public MapData(Map<K, V> payload) {
		this();
		putAll(payload);
	}

	/**
	 * New message with a {@linkplain Map} payload and destination queue.
	 * 
	 * @param payload
	 * @param destination
	 */
	public MapData(Map<K, V> payload, String destination) {
		this(payload);
		setDestination(destination);
	}

	/**
	 * New message with a {@linkplain Map} payload, destination queue and a
	 * correlation ID.
	 * 
	 * @param payload
	 * @param destination
	 * @param corrID
	 */
	public MapData(Map<K, V> payload, String destination, String corrID) {
		this(payload, destination);
		setCorrelationID(corrID);
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		super.writeData(out);
		out.writeInt(sortedMap.size());
		boolean classRecorded = false;
		for(Entry<K, V> entry: sortedMap.entrySet())
		{
			if (!classRecorded) {
				out.writeUTF(entry.getKey().getClass().getName());
				out.writeUTF(entry.getValue().getClass().getName());
				classRecorded = true;
			}
			entry.getKey().writeData(out);
			entry.getValue().writeData(out);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readData(ObjectDataInput in) throws IOException {
		super.readData(in);
		int n = in.readInt();
		if (n > 0) {
			String keyClass = in.readUTF();
			String valClass = in.readUTF();
			Assert.notNull(ApplicationContextWrapper.newInstance(keyClass));
			Assert.notNull(ApplicationContextWrapper.newInstance(valClass));
			K k;
			V v;
			for (int i = 0; i < n; i++) {
				k = (K) ApplicationContextWrapper.newInstance(keyClass);
				v = (V) ApplicationContextWrapper.newInstance(valClass);
				
				k.readData(in);
				v.readData(in);
				
				sortedMap.put(k, v);
			} 
		}
	}

	@Override
	public void clear() {
		sortedMap.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return sortedMap.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return sortedMap.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return sortedMap.entrySet();
	}

	@Override
	public V get(Object key) {
		return sortedMap.get(key);
	}

	@Override
	public boolean isEmpty() {
		return sortedMap.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		return sortedMap.keySet();
	}

	@Override
	public V put(K key, V value) {
		return sortedMap.put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		sortedMap.putAll(m);
	}

	@Override
	public V remove(Object key) {
		return sortedMap.remove(key);
	}

	@Override
	public int size() {
		return sortedMap.size();
	}

	@Override
	public Collection<V> values() {
		return sortedMap.values();
	}

}
