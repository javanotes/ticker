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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.restexpress.exception.DefaultExceptionMapper;
import org.restexpress.exception.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.netty.handler.codec.http.HttpResponseStatus;

@ConditionalOnProperty(havingValue = "true", name = "rest.enable")
@Configuration
public class RestConfiguration {

	@Bean
	@ConfigurationProperties(prefix = "rest.server")
	HandlerMappings mappings()
	{
		return new HandlerMappings();
	}
	static class ServiceExceptionMapper extends DefaultExceptionMapper
	{
		public static final String BADREQ_INV_JSON = "Not a valid json";
		public static final String BADREQ_INV_TEXT = "Not a valid text";
		public static final String BADREQ_INV_JSONARR = "Expecting a json array";
		public static final String REQ_TIMEOUT = "Quorum response not completed";
		
		private static final Logger log = LoggerFactory.getLogger(ServiceExceptionMapper.class);
		
		@Override
		public ServiceException getExceptionFor(Throwable t)
		{
			log.error("--Service exception caught--", t);
			ServiceException ex = new ServiceException(HttpResponseStatus.INTERNAL_SERVER_ERROR, t);
			
			if(t instanceof JsonProcessingException || t instanceof IOException)
			{
				ex = new ServiceException(HttpResponseStatus.BAD_REQUEST, BADREQ_INV_JSON, t);
			}
			else if(t instanceof IllegalArgumentException)
			{
				ex = new ServiceException(HttpResponseStatus.BAD_REQUEST, BADREQ_INV_JSONARR + "/" + BADREQ_INV_TEXT, t);
			}
			else if(t instanceof TimeoutException)
			{
				ex = new ServiceException(HttpResponseStatus.REQUEST_TIMEOUT, REQ_TIMEOUT, t);
			}
			
			return ex;
		}
	}
	@Value("${rest.server.context-path}")
	private String baseUrl;
	@Value("${rest.server.port}")
	private int port;
	
	@ConditionalOnProperty(name = "rest.enable", havingValue = "true")
	@Bean
	RestListener server()
	{
		RestListener server = new RestListener();
		server.setExceptionMap(new ServiceExceptionMapper());
		server.setBaseUrl(baseUrl);
		server.setPort(port);
		return server;
	}
	
	@Bean
	AddHandler add()
	{
		return new AddHandler();
	}
	@Bean
	AppendHandler append()
	{
		return new AppendHandler();
	}
	@Bean
	IngestHandler ingest()
	{
		return new IngestHandler();
	}
	
}
