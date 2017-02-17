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

import org.reactivetechnologies.ticker.messaging.MessageProcessingException;
import org.reactivetechnologies.ticker.messaging.data.TextData;
import org.restexpress.Request;
import org.restexpress.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class AddHandler extends HandlerBase {

	private static final Logger log = LoggerFactory.getLogger(AddHandler.class);
	
	
	public int addJsonToQueue(String queue, String json, boolean block)
			throws JsonProcessingException, IOException, MessageProcessingException {
		ObjectMapper om = new ObjectMapper();
		om.reader().readTree(json);
		log.debug("Adding to queue - [" + queue + "] " + json);
		
		publish(new TextData(json, queue), block);
				
		return 1;
	}

	@Override
	protected void doPost(Request request, Response response) throws Exception {
		RequestBody parsed = parse(request, response);
		addJsonToQueue(parsed.queue, parsed.body, true);
	}
}
