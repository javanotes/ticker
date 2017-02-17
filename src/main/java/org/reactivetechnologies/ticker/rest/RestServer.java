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
package org.reactivetechnologies.ticker.rest;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.List;

import org.restexpress.RestExpress;
import org.restexpress.route.Route;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.embedded.PortInUseException;

import io.netty.channel.Channel;

class RestServer extends RestExpress {

	private static final Logger log = LoggerFactory.getLogger(RestServer.class);

	@Value("${server.port-offset:100}")
	private int MAX_PORT_OFFSET;

	@Autowired
	private AddHandler addService;
	@Autowired
	private AppendHandler appendService;
	@Autowired
	private IngestHandler ingestService;
	
	private static void printRoute(Route r)
	{
		log.debug(r.getMethod()+" ["+r.getPattern()+"] mapped to action "+r.getAction().getDeclaringClass()+"::"+r.getAction().getName());
	}
	private static void printRoutes(List<Route> r)
	{
		for(Route _r : r)
			printRoute(_r);
	}
	public void startServer()
	{
		List<Route> routes = uri(getBaseUrl()+"/add/{queue}", addService).build();
		printRoutes(routes);
		routes = uri(getBaseUrl()+"/ingest/{queue}", ingestService).build();
		printRoutes(routes);
		routes = uri(getBaseUrl()+"/append/{queue}", appendService).build();
		printRoutes(routes);
		
		bind();
		log.info("REST transport started on port "+getPort());
	}
	@Override
	public Channel bind(int port)
	{
		setUseSystemOut(false);
		setPort(getAvailablePort(port));
		if (hasHostname())
		{
			return bind(new InetSocketAddress(getHostname(), port));
		}

		return bind(new InetSocketAddress(port));
	}
	private static boolean isPortAvailable(String host, int port)
	{
		if(host != null)
		{
			try {
				new ServerSocket(port, 50, InetAddress.getByName(host)).close();
				return true;
			} catch (Exception e) {}
		}
		else
		{
			try {
				new ServerSocket(port).close();
				return true;
			} catch (Exception e) {}
		}
		return false;
	}
	private int getAvailablePort(int port) {
		int _port = port;
		String host = null;
		if(hasHostname())
			host = getHostname();
		
		for (int offset = 0; offset < MAX_PORT_OFFSET; offset++) {
			_port += offset;
			if(isPortAvailable(host, _port))
				return _port;
		}
		throw new PortInUseException(port)
		{
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String getMessage() {
				return super.getMessage()+". Unable to find free ports within a range offset of "+MAX_PORT_OFFSET;
			}
		};
	}
	public void stopServer()
	{
		shutdown(true);
		log.info("REST transport stopped..");
	}
	public static void main(String[] args) throws InterruptedException {
		RestServer s = new RestServer();
		s.startServer();
		Thread.sleep(2000);
		s.stopServer();
	}

}
