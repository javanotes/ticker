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
package org.reactivetechnologies.ticker.utils;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

public class ApplicationContextHelper implements ApplicationContextAware{

	public static final String DEFAULT_RESOURCE_PATTERN = "**/*.class";
	private static final Logger log = LoggerFactory.getLogger(ApplicationContextHelper.class);
	
	private static ApplicationContext context;
	/**
	 * Try to instantiate an object for the class name passed. This will not look into 
	 * Spring container and will try to instantiate by standard reflection.
	 * @param className
	 * @return
	 */
	public static Object scanForClassInstance(String className) 
	{
		Assert.notNull(className);
		Object consumer = null;
		if(StringUtils.hasText(className))
		{
			log.debug("Consumer class name specified- "+className);
			consumer = getInstance(className);
		}
		Assert.notNull(consumer, "Unable to instantiate for className- "+className);
		return consumer;
		
	}
	/**
	 * Try to instantiate an object for the class type passed. First it will look into Spring context
	 * for autowired objects, and finally will try to load from classpath. The beanName, if provided, will be used if no 
	 * autowired (by type) instances are found.
	 * @param classType
	 * @param beanName
	 * @return
	 */
	public static <T> Object scanForClassInstance(Class<T> classType, String beanName) 
	{
		Assert.notNull(classType);
		Object consumer = null;
		consumer = scanFromContext(classType);
		if (consumer == null)
		{
			if (StringUtils.hasText(beanName)) {
				consumer = scanFromContext(beanName);
			}
		}
		
		if(consumer == null)
			consumer = performFullScan(classType);
		
		return consumer;
		
	}
	
	private static <T> Object performFullScan(Class<T> classType) 
	{
		log.debug("Performing a full classpath scan to find a first matching class. This the final fallback..");
		PathMatchingResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();
		MetadataReaderFactory metadataReaderFactory = new CachingMetadataReaderFactory(resourceResolver);
		try {
			Resource[] resources = resourceResolver.getResources(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX+DEFAULT_RESOURCE_PATTERN);
			MetadataReader reader;
			String[] ifaces;
			for(Resource res : resources)
			{
				reader = metadataReaderFactory.getMetadataReader(res);
				ifaces = reader.getClassMetadata().getInterfaceNames();
				
				if(ifaces.length > 0)
				{
					Arrays.sort(ifaces);
					if(Arrays.binarySearch(ifaces, classType.getName()) != -1)
					{
						try {
							return newInstance(reader);
						} catch (Throwable e) {
							log.error("Unable to instantiate consumer found in full classpath scan => "+ e.getMessage());
							log.debug("", e);
							return null;
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	private static Object newInstance(MetadataReader reader)
			throws ClassNotFoundException, LinkageError, InstantiationException, IllegalAccessException {
		return ClassUtils.forName(reader.getClassMetadata().getClassName(), Thread.currentThread().getContextClassLoader())
				.newInstance();
	}
	
	
	public static <T> Object scanFromContext(String bean)
	{
		log.debug("Checking for a single matching consumer class from Spring context");
		try {
			return context.getBean(bean);
		} catch (BeansException e) {
			log.debug("No such class found in Spring context");
		}
		return null;
	}
	public static Object getInstance(String className)
	{
		log.debug("Trying to load class");
		Class<?> clazz = classForName(className);
		if(clazz != null)
		{
			log.debug("Class loaded. Checking for instance");
			return getInstance(clazz);
			
		}
		return null;
	}
	
	private static Class<?> classForName(String className)
	{
		try {
			return ClassUtils.forName(className, Thread.currentThread().getContextClassLoader());
		} catch (ClassNotFoundException | LinkageError e) {
			return null;
		}
	}
	/**
	 * 
	 * @param clazz
	 * @return
	 */
	public static <T> Object scanFromContext(Class<?> clazz)
	{
		log.debug("Checking for a single matching consumer class from Spring context");
		try {
			return context.getBean(clazz);
		} catch (BeansException e) {
			log.debug("No such class found in Spring context");
		}
		return null;
	}
	/**
	 * Try to load class from Spring context, if not found then from classpath, if reflect is true.
	 * @param clazz
	 * @param reflect
	 * @return
	 */
	public static Object getInstance(Class<?> clazz)
	{
		log.debug("Trying to do a newInstance using reflection");
		Object instance = null;
		try {
			instance = clazz.newInstance();
		} catch (InstantiationException | IllegalAccessException e1) {
			log.info("Unable to instantiate class. Error => " + e1.getMessage());
			log.debug("", e1);
		} 
		
		return instance;
	}
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		context = applicationContext;
	}
}
