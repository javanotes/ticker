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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.Publisher;
import org.reactivetechnologies.ticker.rest.RestConfiguration.ServiceExceptionMapper;
import org.restexpress.Request;
import org.restexpress.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
/**
 * Abstract base class for creating 'restlets'. 
 * @author esutdal
 *
 */
public abstract class AbstractRestHandler implements RestHandler {

	protected static class RequestBody
	{
		public RequestBody(String queue, String body) {
			super();
			this.queue = queue;
			this.body = body;
		}
		public final String queue;
		public final String body;
	}
	public AbstractRestHandler() {
		super();
	}

	@Autowired
	protected Publisher publisher;
	/* (non-Javadoc)
	 * @see org.reactivetechnologies.ticker.rest.RestHandler#create(org.restexpress.Request, org.restexpress.Response)
	 */
	@Override
	public void create(Request request, Response response) throws Exception
	{
		doPost(request, response);
	}
	/* (non-Javadoc)
	 * @see org.reactivetechnologies.ticker.rest.RestHandler#read(org.restexpress.Request, org.restexpress.Response)
	 */
	@Override
	public void read(Request request, Response response) throws Exception
	{
		doGet(request, response);
	}
	
	/**
	 * Delegate method for GET method.
	 * @param request
	 * @param response
	 * @throws Exception
	 * <p>
	 *  {@link JsonProcessingException},
	 *  {@link IOException},
	 *  {@link IllegalArgumentException} maps to HTTP 400 Bad Request.
	 *  <p>
	 *  {@link TimeoutException} maps to HTTP 408 Service Timeout, anything else maps to HTTP 500 Internal Server Error.
	 *  @see ServiceExceptionMapper
	 */
	protected void doGet(Request request, Response response) throws Exception{response.setResponseNoContent();}
	/* (non-Javadoc)
	 * @see org.reactivetechnologies.ticker.rest.RestHandler#update(org.restexpress.Request, org.restexpress.Response)
	 */
	@Override
	public void update(Request request, Response response) throws Exception
	{
		response.setResponseNoContent();
	}
	/* (non-Javadoc)
	 * @see org.reactivetechnologies.ticker.rest.RestHandler#delete(org.restexpress.Request, org.restexpress.Response)
	 */
	@Override
	public void delete(Request request, Response response) throws Exception
	{
		response.setResponseNoContent();
	}
	/**
	 * Delegate method for POST method.
	 * @param request
	 * @param response
	 * @throws Exception
	 * <p>
	 *  {@link JsonProcessingException},
	 *  {@link IOException},
	 *  {@link IllegalArgumentException} maps to HTTP 400 Bad Request.
	 *  <p>
	 *  {@link TimeoutException} maps to HTTP 408 Service Timeout, anything else maps to HTTP 500 Internal Server Error.
	 *  @see ServiceExceptionMapper
	 */
	protected abstract void doPost(Request request, Response response) throws Exception;
	@Autowired
	protected ActorSystem inbox;
	
	/**
	 * Invoke {@linkplain ActorRef#tell(Object, ActorRef)} using non-blocking/blocking (ask) semantics.
	 * @param d
	 */
	protected void publish(Data d)
	{
		if(d.isAddAsync())
			publisher.ingest(d);
		else
			publisher.offer(d);
		
	}
	/**
	 * Parses a request based on target queue.
	 * @param req
	 * @param res
	 * @return
	 */
	protected RequestBody parse(Request req, Response res) {
		return parse(req, res, RestListener.URL_VAL_QNAME);
	}
	
	protected RequestBody parse(Request req, Response res, String param) {
		String queue = (String) req.getHeader(param);
		Assert.notNull(queue);
		
		ByteBuffer bb = req.getBodyAsByteBuffer();
		byte[] b = new byte[bb.limit()];
		bb.get(b);
		String body = new String(b, StandardCharsets.UTF_8);
		
		return new RequestBody(queue, body);
	}

}