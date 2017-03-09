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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.springframework.util.ClassUtils;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;
/**
 * Utility for class loading from external jar files.
 * @author esutdal
 *
 */
public class JarFileSupport {

	private final JarClassLoader loader;
	public JarFileSupport() {
		this.loader = new JarClassLoader();
	}
	/**
	 * Tries on a best effort basis to load a file.
	 * @param path
	 * @return
	 * @throws FileNotFoundException
	 */
	public static File getAsResource(String path) throws FileNotFoundException
	{
		File f = null;
		try {
			f = ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX+path);
		} catch (FileNotFoundException e) {
			try {
				f = ResourceUtils.getFile(ResourceUtils.FILE_URL_PREFIX+path);
			} catch (FileNotFoundException e1) {
				try {
					ResourceUtils.getFile(Thread.currentThread().getContextClassLoader().getResource(path));
				} catch (Exception e2) {
					FileNotFoundException fnf = new FileNotFoundException();
					fnf.initCause(e2);
					throw fnf;
				}
			}
		}
		return f;
	}
	/**
	 * Utility method to scan for the package declarations under a given jar file. If convention is followed, the first
	 * element of the returned set will be the base package
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static Set<String> scanForPackages(String path) throws IOException {
		try (JarFile file = new JarFile(path)) {
			TreeSet<String> packages = new TreeSet<>(new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					if (o2.length() > o1.length() && o2.contains(o1))
						return -1;
					else if (o2.length() < o1.length() && o1.contains(o2))
						return 1;
					else
						return o1.compareTo(o2);
				}
			});
			for (Enumeration<JarEntry> entries = file.entries(); entries.hasMoreElements();) 
			{
				JarEntry entry = entries.nextElement();
				String name = entry.getName();

				if (name.endsWith(".class")) {
					String fqcn = ClassUtils.convertResourcePathToClassName(name);
					fqcn = StringUtils.delete(fqcn, ".class");
					packages.add(ClassUtils.getPackageName(fqcn));
				}
			}

			return packages;
		}
	}
	/**
	 * Add jars given under the directory to class path.
	 * @param dir
	 * @throws IOException
	 */
	public void deployLibs(String dir) throws IOException
	{
		File f = getAsResource(dir);
		loader.addJars(f);
	}
	/**
	 * Load a given jar file to class path.
	 * @param dir
	 * @throws IOException
	 */
	public void deployLib(String path) throws IOException
	{
		File f = getAsResource(path);
		loader.addJar(f);
	}
	/**
	 * Class.forName() for externally deployed classes.
	 * @param name
	 * @return
	 * @throws ClassNotFoundException
	 */
	public Class<?> classForName(String name) throws ClassNotFoundException
	{
		return loader.loadClassForName(name);
	}
}
