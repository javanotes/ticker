/**
 * Copyright 2016 esutdal

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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

class JarClassLoader extends URLClassLoader {

	private static final Logger log = LoggerFactory.getLogger(JarClassLoader.class);
	public static final String JAR_URL_PREFIX = "jar:file:/";
	public static final String JAR_URL_SUFFIX = "!/";
	/**
	 * 
	 * @param urls
	 * @param delegation
	 */
	public JarClassLoader(URL[] urls, ClassLoader delegation) {
		super(urls, delegation);

	}
	/**
	 * Returns the Class object associated with the class or interface with the given string name.
	 * @see ClassLoader#loadClass(String)
	 * @param className
	 * @return
	 * @throws ClassNotFoundException
	 */
	public Class<?> loadClassForName(String className) throws ClassNotFoundException
	{
		return loadClass(className, true);
	}
	/**
	 * 
	 * @param delegation
	 */
	public JarClassLoader(ClassLoader delegation) {
		this(new URL[] {}, delegation);
	}

	/**
	 * 
	 */
	public JarClassLoader() {
		super(new URL[] {});
	}

	/**
	 * Adds a jar file to the class path. 
	 * 
	 * @param path
	 *            to jar file
	 * @throws IOException
	 */
	private void addJar(String path) throws IOException {
		String urlPath = JAR_URL_PREFIX + path + JAR_URL_SUFFIX;
		URL url = new URL(urlPath);
		addURL(url);
	}

	/**
	 * Adds a jar file to the class path. 
	 * @param jar
	 *            file
	 * @throws IOException
	 */
	public void addJar(File file) throws IOException {
		Assert.isTrue(isValidJar(file), "Not a jar file!");
		addJar(file.getAbsolutePath());
		log.info("Added jar to classpath -> "+file.getName());
	}
	/**
	 * 
	 * @param f
	 * @return
	 */
	private static boolean isValidJar(File f)
	{
		try(JarFile j = new JarFile(f))
		{
			for (Enumeration<JarEntry> entries = j.entries(); entries.hasMoreElements();) 
			{
				entries.nextElement();
			}
		} 
		catch (IOException e) {
			return false;
		}
		return true;
	}
	/**
	 * Add JAR files recursively, found in the given directory to this class loader.
	 * 
	 * @param root
	 *            The directory to recursively search for JAR files.
	 * @throws IOException 
	 */
	public void addJars(File root) throws IOException {
		Assert.notNull(root);
		if(!root.isDirectory())
			throw new IOException(root+" is not a directory");
		
		File[] children = root.listFiles();
		if (children == null) {
			return;
		}
		log.info("Start traversing root ["+root+"] for jar files");
		for (int i = 0; i < children.length; i++) {
			File child = children[i];
			if (child.isDirectory() && child.canRead()) {
				addJars(child);
			} 
			else {
				try {
					addJar(child);
				} catch (IllegalArgumentException e) {
					// ignore invalid files
				}
			}
		}
		log.info("* End of traversal *");
	}
}
