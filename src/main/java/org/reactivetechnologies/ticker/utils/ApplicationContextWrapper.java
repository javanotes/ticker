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
/**
 * Utility class which wraps a Spring {@linkplain ApplicationContext} and provide support
 * for object instantiations.
 */
public class ApplicationContextWrapper implements ApplicationContextAware{

	public static final String DEFAULT_RESOURCE_PATTERN = "**/*.class";
	private static final Logger log = LoggerFactory.getLogger(ApplicationContextWrapper.class);
	
	private static ApplicationContext context;
	
	/**
	 * Try to instantiate an object for the class type passed. Initially it will look into Spring context
	 * for autowired objects; first by Type, then by Name if beanName is provided. 
	 * Finally will try to scan from class-path and return a new instance, if instance not found in Spring context. 
	 * @param classType the class type to instantiate
	 * @param beanName if provided, should match with a Spring bean by name
	 * @return
	 */
	public static <T> Object getInstance(Class<T> classType, String beanName) 
	{
		Assert.notNull(classType);
		Object consumer = null;
		log.info("Scanning classpath for an instance of " + classType+ (beanName != null ? ", with identifier '"+beanName+"'" : ""));
		consumer = getInstance(classType);
		if (consumer == null)
		{
			if (StringUtils.hasText(beanName)) {
				consumer = getInstance(beanName);
			}
		}
		
		if(consumer == null)
			consumer = performFullScan(classType);
		
		log.debug("End scan. Found match? "+(consumer != null));
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
							log.warn("Unable to instantiate consumer found in full classpath scan => "+ e.getMessage());
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
		return newInstance(reader.getClassMetadata().getClassName());
	}

	/**
	 * Get instance by name from Spring context.
	 * @param bean
	 * @return
	 */
	public static <T> Object getInstance(String bean)
	{
		log.debug("Checking for a single matching consumer bean (by name) from Spring context");
		try {
			return context.getBean(bean);
		} catch (BeansException e) {
			log.debug("No such class found in Spring context");
		}
		return null;
	}
	/**
	 * Invoke {@linkplain Class#newInstance()}.
	 * @param className
	 * @return
	 */
	public static Object newInstance(String className)
	{
		Assert.notNull(className);
		log.debug("Trying to load class using reflection");
		Class<?> _class = null;
		if(StringUtils.hasText(className))
		{
			log.debug("Consumer class name specified- "+className);
			_class = classForName(className);
		}
		Assert.notNull(_class, "Unable to instantiate for className- "+className);
		return newInstance(_class);
	}
	
	private static Class<?> classForName(String className)
	{
		try {
			return ClassUtils.forName(className, null);
		} catch (ClassNotFoundException | LinkageError e) {
			return null;
		}
	}
	/**
	 * Get instance by type from Spring context.
	 * @param clazz
	 * @return
	 */
	public static <T> Object getInstance(Class<?> clazz)
	{
		log.debug("Checking for a single matching consumer bean (by type) from Spring context");
		try {
			return context.getBean(clazz);
		} catch (BeansException e) {
			log.debug("No such class found in Spring context");
		}
		return null;
	}
	/**
	 * Invoke {@linkplain Class#newInstance()}.
	 * @param clazz
	 * @param reflect
	 * @return
	 */
	public static Object newInstance(Class<?> clazz)
	{
		log.debug("Trying to do a newInstance using reflection");
		Object instance = null;
		try {
			instance = ClassUtils.getUserClass(clazz).newInstance();
		} catch (InstantiationException | IllegalAccessException e1) {
			log.debug("Unable to instantiate class. Error => " + e1.getMessage());
			log.debug("", e1);
		} 
		
		return instance;
	}
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		context = applicationContext;
	}
}
