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

import java.util.Map.Entry;
import java.util.Observable;
import java.util.Set;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.query.Predicate;

public abstract class AbstractMigratedPartitionListener implements MigratedPartitionListener {

	@Override
	public void update(Observable o, Object event) {
		if(event instanceof MigrationEvent)
			fireOnMigration((MigrationEvent) event);
	}

	protected HazelcastInstance hazelcast;
	@Override
	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.hazelcast = hazelcastInstance;
	}
	
	private void fireOnMigration(MigrationEvent event) 
	{
		IMap<Object, Object> map;
		for(String mapName : processingMaps())
		{
			map = hazelcast.getMap(mapName);
			Set<Object> migratedKeySet = map.localKeySet(new Predicate<Object, Object>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public boolean apply(Entry<Object, Object> mapEntry) {
					return event.getPartitionId() == hazelcast.getPartitionService().getPartition(mapEntry.getKey())
							.getPartitionId();
				}

				
			});
			
			onMigratedKeySet(map, migratedKeySet);
		}
	}
	
}
