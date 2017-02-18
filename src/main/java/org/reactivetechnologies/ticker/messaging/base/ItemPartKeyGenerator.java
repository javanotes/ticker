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

import org.reactivetechnologies.ticker.datagrid.HazelcastOperations;
import org.springframework.beans.factory.annotation.Autowired;

import com.hazelcast.util.UuidUtil;

public class ItemPartKeyGenerator {

	@Autowired
	HazelcastOperations hazelOps;
	//TODO: key generation strategy
	/**
	 * Get the next partitioning key for distributing queue item.
	 * @return
	 */
	public String getNext()
	{
		return UuidUtil.createClusterUuid();
	}
	/**
	 * Get the next partitioning key for distributing queue item.
	 * @param key queue name
	 * @return
	 */
	public long getNext(String key)
	{
		return hazelOps.hazelcastInstance().getIdGenerator(key).newId();
	}
}
