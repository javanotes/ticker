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

import org.reactivetechnologies.ticker.messaging.base.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.parser.proto.messages.AbstractMessage.QOSType;

class PublishDataDistributor extends TransportInterceptor {
	
	private static final Logger LOG = LoggerFactory.getLogger(PublishDataDistributor.class);
	
	@Autowired
	private Publisher tickerPub;
	
	@Override
    public void onPublish(InterceptPublishMessage msg) {
		MqttData data = new MqttData(msg);
		if(msg.getQos() == QOSType.MOST_ONE)
			tickerPub.ingest(data);//fire and forget
		else
			tickerPub.offer(data);
		
		LOG.debug("Fired MQTT request "+data+" Payload len: "+data.getPayload().length);
	}
}
