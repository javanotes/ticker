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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.reactivetechnologies.io.moquette.server.Server;
import org.reactivetechnologies.ticker.utils.CommonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.PortInUseException;

import io.moquette.BrokerConstants;
import io.moquette.interception.InterceptHandler;
import io.moquette.server.config.IConfig;
import io.moquette.spi.security.IAuthenticator;
import io.moquette.spi.security.IAuthorizator;
import io.moquette.spi.security.ISslContextCreator;

class MqttListener extends Server {

	private static Logger log = LoggerFactory.getLogger(MqttListener.class);
	@Autowired
	TransportInterceptor interceptor;
	@Autowired
	private IConfig cfg;

	
	@PostConstruct
	public void init() throws IOException 
	{
		startServer(cfg, Arrays.asList(interceptor));
	}

	@PreDestroy
	public void destroy() {
		stopServer();
		log.info("MQTT transport stopped..");
	}

	@Override
	public void startServer(IConfig config, List<? extends InterceptHandler> handlers, ISslContextCreator sslCtxCreator,
			IAuthenticator authenticator, IAuthorizator authorizator) throws IOException {
		preServerStart(config);
		super.startServer(config, handlers, sslCtxCreator, authenticator, authorizator);
		postServerStart();
	}
	private int port;
	private void preServerStart(IConfig config) 
	{
		port = Integer.valueOf(config.getProperty(BrokerConstants.PORT_PROPERTY_NAME));
		port = CommonHelper.scanAvailablePort(port, 100, config.getProperty(BrokerConstants.HOST_PROPERTY_NAME));
		if(port != -1)
		{
			config.setProperty(BrokerConstants.PORT_PROPERTY_NAME, port+"");
			return;
		}
		throw new PortInUseException(port)
		{
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String getMessage() {
				return super.getMessage()+". Unable to find free ports within a range offset of "+100;
			}
		};
	}

	private void postServerStart() {
		log.info("MQTT transport started on port "+port);
	}

}
