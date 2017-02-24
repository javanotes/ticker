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
package org.reactivetechnologies.ticker.scheduler;

import org.reactivetechnologies.ticker.datagrid.HazelcastConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.ErrorHandler;

@Configuration
@Import(HazelcastConfiguration.class)
public class SchedulerConfiguration {

	private static final Logger log = LoggerFactory.getLogger(SchedulerConfiguration.class);
	@Value("${scheduler.threadPoolSize:4}")
	private int poolSize;
	@Value("${scheduler.awaitTerminationSeconds:0}")
	private int awaitTerminationSeconds;
	
	@Bean
	@DependsOn("scheduler")
	public TaskScheduler schedulerFacade()
	{
		return new TaskSchedulerService();
	}
	@Bean
	ThreadPoolTaskScheduler scheduler()
	{
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setPoolSize(poolSize);
		scheduler.setRemoveOnCancelPolicy(true);
		if (awaitTerminationSeconds > 0) {
			scheduler.setWaitForTasksToCompleteOnShutdown(true);
			scheduler.setAwaitTerminationSeconds(awaitTerminationSeconds);
		}
		else
			scheduler.setWaitForTasksToCompleteOnShutdown(false);
		
		scheduler.setThreadGroupName("TickerScheduler");
		scheduler.setThreadNamePrefix("__scheduler.");
		scheduler.setErrorHandler(new ErrorHandler() {
			
			@Override
			public void handleError(Throwable t) {
				log.error("-- Error stacktrace --", t);
			}
		});
		
		log.info("Scheduler configured with "+poolSize+" threads");
		return scheduler;
	}
}
