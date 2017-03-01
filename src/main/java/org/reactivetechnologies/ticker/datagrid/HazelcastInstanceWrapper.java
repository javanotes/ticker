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

import java.io.IOException;
import java.net.URL;
import java.util.Set;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastProperties;
import org.springframework.core.io.Resource;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
class HazelcastInstanceWrapper {

	private static final Logger log = LoggerFactory.getLogger(HazelcastInstanceWrapper.class);
	static final String HZ_MAP_SERVICE = "hz:impl:mapService";
	public static final String NODE_INSTANCE_ID = "keyval.hazelcast.id";
	
	private static final String groupName = "Ticker";
	private static final String groupPwd = "@dmIn123#";
	
	private final HazelcastProperties hazelcastProperties;
	
	HazelcastInstance hazelcast;
	private String instanceId;
	
	public String getInstanceId() {
		return instanceId;
	}

	private volatile boolean started = false;
	
	
	private static Config resolveConfigLocation(Resource configLocation) throws IOException {
		URL configUrl = configLocation.getURL();
		Config config = new XmlConfigBuilder(configUrl).build();
		if (ResourceUtils.isFileURL(configUrl)) {
			config.setConfigurationFile(configLocation.getFile());
		}
		else {
			config.setConfigurationUrl(configUrl);
		}
		return config;
	}
	private boolean hasInstanceId;
	/**
	 * Set properties and etc..
	 * @param hzConfig
	 */
	private void setInstanceProperties(Config hzConfig)
	{
		if (!StringUtils.hasText(hzConfig.getGroupConfig().getName()))
			hzConfig.getGroupConfig().setName(groupName);
		if (!StringUtils.hasText(hzConfig.getGroupConfig().getPassword()))
			hzConfig.getGroupConfig().setPassword(groupPwd);
		
		hzConfig.setProperty("hazelcast.shutdownhook.enabled", "false");
		if(StringUtils.hasText(instanceId))
		{
			hzConfig.setInstanceName(instanceId);
			hzConfig.getMemberAttributeConfig().setStringAttribute(NODE_INSTANCE_ID, instanceId);
			hasInstanceId = true;
		}
	}
	private HazelcastInstance getHazelcastInstance() throws IOException 
	{
		log.info(">> Hazelcast startup sequence initiated");
		Resource config = this.hazelcastProperties.resolveConfigLocation();
		Config hzConfig = new XmlConfigBuilder().build();
		if (config != null) 
		{
			hzConfig = resolveConfigLocation(config);
			
		}
		else
		{
			hzConfig.getGroupConfig().setName(groupName);
			hzConfig.getGroupConfig().setPassword(groupPwd);
		}
		setInstanceProperties(hzConfig);
		return Hazelcast.newHazelcastInstance(hzConfig);
	}

	/**
	 * Start {@linkplain HazelcastInstance} and join the cluster with a given instance id.
	 * @param instanceId
	 *            instance name
	 	@throws BeanInitializationException
	 	
	 */
	public void start(String instanceId) {
		if(!started)
		{
			synchronized (this) {
				if(!started)
				{
					this.instanceId = instanceId;
					start0();
				}
			}
		}
		
		
	}
	/**
	 * Start {@linkplain HazelcastInstance} and join the cluster with a random instance id.
	 * @throws BeanInitializationException
	 */
	public void start() {
		start("");		
	}
	private void start0() {
		try 
		{
			hazelcast = getHazelcastInstance();
			if (hasInstanceId) {
				verifyInstance();
			}
			joinCluster();
			started = true;
		} 
		catch (Exception e) {
			stop();
			throw new BeanInitializationException("Unable to start Hazelcast instance", e);
		}
		
	}
	private void verifyInstance() 
	{
		Set<Member> members = hazelcast.getCluster().getMembers();

		int memberIdCnt = 0;
		for (Member m : members) {

			if (m.getStringAttribute(NODE_INSTANCE_ID).equals(getInstanceId())) {
				memberIdCnt++;
			}
			if (memberIdCnt >= 2) {
				stop();
				throw new IllegalStateException(
						"Instance not allowed to join cluster as [" + getInstanceId() + "]. Duplicate instance id.");
			}

		}
		
	}

	private HazelcastClusterListener clusterListener;
	public HazelcastClusterListener getClusterListener() {
		return clusterListener;
	}
	/**
	 * Package level constructor.
	 * @param hazelcastProperties 
	 */
	HazelcastInstanceWrapper(HazelcastProperties hazelcastProperties) {
		this.hazelcastProperties = hazelcastProperties;
	}
	@PreDestroy
	public void stop() {
		log.warn("Shutting down Hazelcast instance ..");
		if (hazelcast != null && hazelcast.getLifecycleService().isRunning()){
			hazelcast.getLifecycleService().shutdown();
			started = false;
		}
	}
	
	private void joinCluster()
	{
		log.info("Waiting for node to balance..");
	    clusterListener = new HazelcastClusterListener(hazelcast);
	    
	    hazelcast.getCluster().addMembershipListener(clusterListener);
	    hazelcast.getPartitionService().addMigrationListener(clusterListener);
	    hazelcast.getPartitionService().addPartitionLostListener(clusterListener);
	    log.info("Registered cluster listeners");
	    
	    log.info("---------------------------------------------");
		log.info("Hazelcast system initialized");
	    log.info("Joined cluster with groupId [" + hazelcast.getConfig().getGroupConfig().getName()
	                          				+ "] " + (StringUtils.hasText(instanceId) ? "and instanceId [" + instanceId +"]" : ""));
	    log.info("---------------------------------------------");

	}

	public boolean isStarted() {
		return started;
	}
	
}
