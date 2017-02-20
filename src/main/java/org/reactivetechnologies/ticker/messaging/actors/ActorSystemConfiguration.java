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
package org.reactivetechnologies.ticker.messaging.actors;

import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.reactivetechnologies.ticker.datagrid.HazelcastConfiguration;
import org.reactivetechnologies.ticker.datagrid.HazelcastOperations;
import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.DeadLetterHandler;
import org.reactivetechnologies.ticker.messaging.base.ItemPartKeyGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import akka.actor.Props;
import akka.japi.Creator;
import akka.routing.RoundRobinPool;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

@Configuration
@Import(HazelcastConfiguration.class)
@DependsOn({"hzWrapper"})
public class ActorSystemConfiguration {
	
	static final Logger log = LoggerFactory.getLogger(ActorSystemConfiguration.class);
	final static Config akkaConfig = ConfigFactory.parseString(
			
			"akka-actor.ticker-dispatcher.type = Dispatcher \n" +
			"akka-actor.ticker-dispatcher.executor = fork-join-executor \n" +
			"akka-actor.ticker-dispatcher.fork-join-executor.parallelism-min = 8 \n" +
			"akka-actor.ticker-dispatcher.fork-join-executor.parallelism-factor = 3.0 \n" +
			"akka-actor.ticker-dispatcher.fork-join-executor.parallelism-max = 64 \n" 
			);

	@Autowired
	HazelcastOperations ops;
	private ActorSystemWrapper wrapper;
	
	private class ActorSystemWrapper
	{
		final ActorSystem akka;

		ActorSystemWrapper() {
			super();
			this.akka = ActorSystem.create("ticker", ActorSystemConfiguration.akkaConfig);
		}
		
		void destroy() throws Exception
		{
			Await.result(akka.terminate(), Duration.create(10, TimeUnit.SECONDS));
			ActorSystemConfiguration.log.info("Actor system stopped..");
		}
	}
	
	public ActorSystemConfiguration() {
		
	}
	@Value("${pub.concurrency:1}")
	private int pubConcurrency;
	@Value("${pub.sync.timeout:1000}")
	private int pubTimeout;
	@Bean
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_SINGLETON)
	ActorSystem actors()
	{
		return wrapper.akka;
	}
	
	@PostConstruct
	private void init()
	{
		wrapper = new ActorSystemWrapper();
		ops.addMigrationListener(messageContainerSupport());
		log.info("Actor system initialized..");
	}
	@PreDestroy
	private void destroy() throws Exception
	{
		wrapper.destroy();
	}
	
	//publisher actor can be pooled
	@Bean
	<T extends Data> ActorRef publisherActor()
	{
		return actors().actorOf(new RoundRobinPool(pubConcurrency).props(Props.create(pubCreator())));
	}
		
	@Bean
	PublisherCreator pubCreator()
	{
		return new PublisherCreator();
	}
	@Bean
	ContainerCreator subCreator()
	{
		return new ContainerCreator();
	}
	public static class PublisherCreator implements Creator<QueuePublisherActor>
	{

		@Autowired
		HazelcastOperations ops;
		@Autowired ItemPartKeyGenerator keyGen;
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public QueuePublisherActor create() throws Exception {
			QueuePublisherActor pub = new QueuePublisherActor(ops);
			pub.setKeyGen(keyGen);
			return pub;
		}
		
	}
	public static class ContainerCreator implements Creator<QueueContainerActor>
	{
		@Autowired MessagingContainerSupport migrListener;
		@Autowired DeadLetterHandler dlHandler;
		@Autowired
		HazelcastOperations ops;
		@Value("${container.clear_all_pending:false}")
		private boolean clearAllPendingEntries;
		@Value("${container.remove_entry_immediate:false}")
		private boolean removeImmediate;
		@Value("${container.process_entry_exclusive:false}")
		private boolean checkExclusiveAccess;
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		@Override
		public QueueContainerActor create() throws Exception {
			QueueContainerActor qc = new QueueContainerActor(ops);
			qc.setClearAllPendingEntries(clearAllPendingEntries);
			if(qc.isClearAllPendingEntries())
				log.warn("'clear_all_pending' is set to TRUE. This is not advisable in production environment!");
			qc.setRemoveImmediate(removeImmediate);
			qc.setCheckExclusiveAccess(checkExclusiveAccess);
			qc.migrationListener = migrListener;
			qc.deadLetterHandler = dlHandler;
			return qc;
		}
		
	}
	@Bean
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	Inbox inbox()
	{
		return Inbox.create(actors());
	}
	
	//container actor is single instance
	@Bean
	<T extends Data> ActorRef containerActor()
	{
		return actors().actorOf(Props.create(subCreator()));
	}
	

	@Bean
	MessagingContainerSupport messageContainerSupport()
	{
		return new MessagingContainerSupport();
	}
	
}
