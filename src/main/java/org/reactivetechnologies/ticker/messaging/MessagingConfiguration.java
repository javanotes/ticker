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
package org.reactivetechnologies.ticker.messaging;

import org.reactivetechnologies.ticker.datagrid.HazelcastOperations;
import org.reactivetechnologies.ticker.messaging.actors.ActorSystemConfiguration;
import org.reactivetechnologies.ticker.messaging.base.DeadLetterHandler;
import org.reactivetechnologies.ticker.messaging.base.DefaultPublisher;
import org.reactivetechnologies.ticker.messaging.base.ItemPartKeyGenerator;
import org.reactivetechnologies.ticker.messaging.base.ringbuff.RingBufferedQueueContainer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@SuppressWarnings("deprecation")
@Configuration
@Import(ActorSystemConfiguration.class)
public class MessagingConfiguration {

	@Bean
	DefaultPublisher defPub(HazelcastOperations hazelWrap)
	{
		return new DefaultPublisher(hazelWrap);
	}
	@Bean
	ItemPartKeyGenerator keyGen()
	{
		return new ItemPartKeyGenerator();
	}
	RingBufferedQueueContainer ringBuffQueueContainer(HazelcastOperations hazelWrap)
	{
		return new RingBufferedQueueContainer(hazelWrap);
	}
	@Bean
	DeadLetterHandler deadLetter()
	{
		return new DeadLetterHandler() {
			
			@Override
			public void handle(Data d) {
				// TODO override and implement
				System.out.println("new DeadLetterHandler() {...}.handle()");
			}
		};
	}
}
