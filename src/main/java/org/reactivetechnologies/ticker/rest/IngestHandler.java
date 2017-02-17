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
import java.util.Iterator;

import org.reactivetechnologies.ticker.messaging.MessageProcessingException;
import org.reactivetechnologies.ticker.messaging.data.TextData;
import org.restexpress.Request;
import org.restexpress.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
/**
 * Add JSON array to queue in an asynchronous manner.
 * @author esutdal
 *
 */
public class IngestHandler extends HandlerBase{

	private static final Logger log = LoggerFactory.getLogger(AppendHandler.class);

	/**
	 * 
	 * @param queue
	 * @param jsonArray
	 * @throws JsonProcessingException
	 * @throws IOException
	 * @throws MessageProcessingException
	 */
	public void addJsonArrayToQueue(String queue, String jsonArray)
			throws JsonProcessingException, IOException, MessageProcessingException {
		ObjectMapper om = new ObjectMapper();
		JsonNode root = om.reader().readTree(jsonArray);
		Assert.isTrue(root.isArray(), "Not a JSON array");
		JsonNode each;
		ObjectWriter ow = om.writer();

		log.debug("Adding to queue - [" + queue + "] ");

		try 
		{
			for (Iterator<JsonNode> iter = root.elements(); iter.hasNext();) {
				each = iter.next();
				
				TextData text = new TextData(ow.writeValueAsString(each), queue);
				text.setAddAsync(true);
				publish(text, false);
			}
		}

		catch (Exception e) {
			throw new MessageProcessingException(e);
		}
	}

	@Override
	protected void doPost(Request request, Response response) throws Exception {
		RequestBody parsed = parse(request, response);
		addJsonArrayToQueue(parsed.queue, parsed.body);
	}



}
