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
package org.reactivetechnologies.ticker.mqtt;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import io.moquette.server.config.FileResourceLoader;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import io.moquette.server.config.ResourceLoaderConfig;
import io.moquette.spi.impl.ProtocolProcessorBootstrapper__;
import io.moquette.spi.persistence.HazelcastPersistentStore;

@ConditionalOnProperty(name = "mqtt.enable", havingValue = "true")
@Configuration
public class MqttConfiguration {

	@ConditionalOnProperty(name = "mqtt.enable", havingValue = "true")
	@Bean
	MqttListener mqtt()
	{
		return new MqttListener();
	}
	@Bean
	PublishDataDistributor mqttHandler()
	{
		return new PublishDataDistributor();
	}
	
	@Bean
	IConfig config() throws FileNotFoundException
	{
		IConfig cfg = null;
		if(StringUtils.hasText(conf))
		{
			File f = ResourceUtils.getFile(conf);
			cfg = new ResourceLoaderConfig(new FileResourceLoader(f));
		}
		else
			cfg = new MemoryConfig(new Properties());//default config
		
		return cfg;
	}
	
	@Bean
	ProtocolProcessorBootstrapper__ processorBoot()
	{
		return new ProtocolProcessorBootstrapper__();
	}
	
	@Bean
	HazelcastPersistentStore mapStore() throws FileNotFoundException
	{
		return new HazelcastPersistentStore(config());
	}
	@Value("${mqtt.conf.file:}")
	private String conf;
}
