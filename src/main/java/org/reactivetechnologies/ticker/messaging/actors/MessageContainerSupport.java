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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.reactivetechnologies.ticker.datagrid.AbstractMigratedPartitionListener;
import org.reactivetechnologies.ticker.messaging.Data;
import org.reactivetechnologies.ticker.messaging.base.AbstractQueueListener;
import org.reactivetechnologies.ticker.messaging.base.QueueListener;
import org.reactivetechnologies.ticker.messaging.data.DataWrapper;
import org.reactivetechnologies.ticker.messaging.dto.__EntryRequest;
import org.reactivetechnologies.ticker.messaging.dto.__RegistrationRequest;
import org.reactivetechnologies.ticker.messaging.dto.__RunRequest;
import org.reactivetechnologies.ticker.messaging.dto.__StopRequest;
import org.reactivetechnologies.ticker.utils.ApplicationContextWrapper;
import org.reactivetechnologies.ticker.utils.JarFileSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.hazelcast.core.IMap;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import scala.concurrent.duration.FiniteDuration;

/**
 * A supporting class for interacting with the underlying
 * {@linkplain QueueContainerActor}.
 * 
 * @author esutdal
 *
 */
public class MessageContainerSupport extends AbstractMigratedPartitionListener implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(MessageContainerSupport.class);
	@Autowired
	private ActorSystem actors;

	@Autowired
	@Qualifier("containerActor")
	private ActorRef containerActor;

	@Override
	public String[] processingMaps() {
		String[] _arr = new String[queueMaps.size()];
		return queueMaps.toArray(_arr);
	}

	public void addProcessingMap(String map) {
		queueMaps.add(map);
	}

	private final List<String> queueMaps = Collections.synchronizedList(new ArrayList<>());

	@Override
	public void onMigratedKeySet(IMap<Object, Object> map, Set<Object> migratedKeySet) {
		for (Object key : migratedKeySet) {
			DataWrapper c = new DataWrapper((Data) map.get(key), false, (Serializable) key);
			containerActor.tell(new __EntryRequest(c), ActorRef.noSender());

			log.info("Submitted migrated entry for key -> " + key);
		}

	}

	/**
	 * Register a new {@linkplain QueueListener} to the container.
	 * 
	 * @param listener
	 */
	public <T extends Data> void registerListener(QueueListener<T> listener) {
		Inbox in = Inbox.create(actors);
		in.send(containerActor, new __RegistrationRequest<>(listener));
		try {
			in.receive(FiniteDuration.apply(10, TimeUnit.SECONDS));
		} catch (TimeoutException e) {
			throw new BeanCreationException("Operation failed", e);
		}

	}

	/**
	 * Start container if not already started.
	 */
	public void start() {
		log.info("Container initialization signalled..");
		containerActor.tell(new __RunRequest(), actors.guardian());
	}

	/**
	 * Gracefully stop container if started, awaiting for the given amount of
	 * time. If not successful within that time, then force stop.
	 * <p>
	 * Ideally, stopping the container need not be handled externally. It will
	 * try to recover from failures, however, will stop automatically on
	 * irrecoverable scenarios (as per Actor pattern).
	 * 
	 * @param timeout
	 * @param unit
	 */
	public void stop(long timeout, TimeUnit unit) {
		Inbox in = Inbox.create(actors);
		in.send(containerActor, new __StopRequest(timeout, unit));
		boolean stopped = false;
		try {
			stopped = (boolean) in.receive(FiniteDuration.apply(timeout, unit));
		} catch (TimeoutException e) {

		}
		if (!stopped)
			actors.stop(containerActor);
	}

	@SuppressWarnings("unchecked")
	private void registerAndStart() {
		log.warn(
				"No deployment directory specified. Checking for a single matching AbstractQueueListener implementation");
		Object consumer = ApplicationContextWrapper.getInstance(AbstractQueueListener.class, null);
		Assert.notNull(consumer);

		try {
			registerListener((QueueListener<? extends Data>) consumer);
			log.info(">> Consumer auto registration complete [" + consumer.getClass().getName() + "]");
			start();
		} catch (Exception e) {
			log.error("Invalid implementation of QueueListener. See nested exception");
			throw e;
		}
	}

	private void deployAndStart() throws Exception {
		log.info(">> Checking for deployments at- " + deployDir);
		deploy();
		start();
	}
	/**
	 * Start container only if any deployable is detected. else leave it to clients to manually start the container.
	 * This is needed in order for listeners to be registered properly. A listener will not be registered to an already started container.
	 * @throws Exception
	 */
	private void doRun() throws Exception 
	{
		if (StringUtils.hasText(deployDir)) {
			deployAndStart();
		} else {
			try {
				registerAndStart();
			} catch (IllegalArgumentException e) {
				log.warn("*** No consumer found for deployment ***");
			}

		}
	}

	@Override
	public void run(String... args) throws Exception {
		doRun();
	}

	private Class<?>[] loadClasses() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String[] classes = null;
		if (className.contains(",")) {
			classes = className.split(",");
		} else {
			classes = new String[] { className };
		}
		Class<?>[] listeners = new Class<?>[classes.length];
		for (int i = 0; i < classes.length; i++) {
			listeners[i] = deployer.classForName(classes[i]);
		}

		return listeners;

	}

	private void deployLibs() throws IOException {
		String[] dirs = null;
		if (deployDir.contains(",")) {
			dirs = deployDir.split(",");
		} else {
			dirs = new String[] { deployDir };
		}
		for (int i = 0; i < dirs.length; i++) {
			String dir = dirs[i];
			deployer.deployLibs(dir);
		}

	}

	@SuppressWarnings("unchecked")
	private <T extends Data> void deploy() throws Exception {
		try {
			deployLibs();
			Class<?>[] consumers = loadClasses();
			for (Class<?> c : consumers)
				registerListener((QueueListener<T>) ApplicationContextWrapper.newInstance(c));

			log.info("Deployment complete for consumer");
		} catch (IOException e) {
			log.error("Unable to deploy jar. See nested exception");
			throw e;
		} catch (ReflectiveOperationException e) {
			log.error("Unable to find impl classes in deployment. See nested exception");
			throw e;
		} catch (ClassCastException e) {
			log.error("Invalid implementation of QueueListener. See nested exception");
			throw e;
		}
	}

	@Autowired
	private JarFileSupport deployer;

	@Value("${container.deploy.dir:}")
	private String deployDir;
	@Value("${container.deploy.consumer_class:}")
	private String className;

}
