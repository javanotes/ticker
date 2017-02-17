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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.Publisher;
import org.restexpress.Request;
import org.restexpress.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

abstract class HandlerBase {

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
	public HandlerBase() {
		super();
	}

	@Autowired
	protected Publisher publisher;
		
	public void create(Request request, Response response) throws Exception
	{
		doPost(request, response);
		response.setResponseCreated();
	}
	public void read(Request request, Response response) throws Exception
	{
		
	}
	public void update(Request request, Response response) throws Exception
	{
		
	}
	public void delete(Request request, Response response) throws Exception
	{
		
	}
	protected abstract void doPost(Request request, Response response) throws Exception;
	@Autowired
	protected ActorSystem inbox;
	
	/**
	 * Invoke {@linkplain ActorRef#tell(Object, ActorRef)} using non-blocking/blocking (ask) semantics.
	 * @param d
	 * @param ask
	 */
	protected void publish(Data d, boolean ask)
	{/*
		if(ask)
		{
			Inbox in = Inbox.create(inbox);
			in.send(publisher, d);
			try {
				in.receive(FiniteDuration.apply(1, TimeUnit.SECONDS));
			} catch (TimeoutException e) {
				throw new DataAccessResourceFailureException("Ask timed out", e);
			}
		}
		else
			publisher.tell(d, ActorRef.noSender());
	*/
		
		if(d.isAddAsync())
			publisher.ingest(d);
		else
			publisher.offer(d);
		
	}
	protected RequestBody parse(Request req, Response res) {
		String queue = (String) req.getHeader("queue");
		Assert.notNull(queue);
		
		ByteBuffer bb = req.getBodyAsByteBuffer();
		byte[] b = new byte[bb.limit()];
		bb.get(b);
		String body = new String(b, StandardCharsets.UTF_8);
		
		return new RequestBody(queue, body);
	}

}