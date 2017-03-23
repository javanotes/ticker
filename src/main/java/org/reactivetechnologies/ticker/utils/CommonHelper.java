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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.reactivetechnologies.ticker.messaging.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.JavaDefaultSerializers.JavaSerializer;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
@Component
public class CommonHelper {
	public CommonHelper() {
	}

	@Autowired
	InternalSerializationService serializeService;
	private static JavaSerializer javaSerializer;
	static
	{
		javaSerializer = new JavaSerializer(true, false);
	}
	/**
	 * Encode a class name to a repeatable pattern string.
	 * @param _class
	 * @return
	 */
	public static String encodeClassName(Class<?> _class)
	{
		String[] pkgParts =StringUtils.delimitedListToStringArray(ClassUtils.getPackageName(_class), ".");
		int i = 0;
		StringBuilder sb = new StringBuilder();
		for (; i < pkgParts.length-1; i++) {
			String string = pkgParts[i];
			sb.append(string.charAt(0)).append("_");
		}
		sb.append(pkgParts[i].toLowerCase()).append("_").append(_class.getSimpleName().toLowerCase());
		return sb.toString();
	}
	/**
	 * Clone via java serialization.
	 * @param orig
	 * @return
	 * @throws IOException
	 * 
	 */
	public Data deepCopy(Data orig) throws IOException
	{
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		ObjectDataOutput out = new ObjectDataOutputStream(new ByteArrayOutputStream(), serializeService);
		try 
		{
			javaSerializer.write(out, orig);
			ObjectDataInputStream in = new ObjectDataInputStream(new ByteArrayInputStream(bytes.toByteArray()), null);
			return (Data) javaSerializer.read(in);
		} catch (IOException e) {
			throw e;
		}
	}
	/**
	 * Serialize to byte[].
	 * @param d
	 * @return
	 * @throws IOException
	 */
	public byte[] marshall(DataSerializable d) throws IOException
	{
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		ObjectDataOutput out = new ObjectDataOutputStream(bytes, serializeService);
		d.writeData(out);
		return bytes.toByteArray();
	}
	/**
	 * Populate from byte[].
	 * @param b
	 * @param ser
	 * @throws IOException
	 */
	public void unmarshall(byte[] b, DataSerializable ser) throws IOException
	{
		ObjectDataInputStream in = new ObjectDataInputStream(new ByteArrayInputStream(b), serializeService);
		ser.readData(in);
		
	}
	
	private static boolean isPortAvailable(String host, int port)
	{
		if(host != null)
		{
			try 
			{
				new ServerSocket(port, 50, InetAddress.getByName(host)).close();
				return true;
			} 
			catch (Exception e) {}
		}
		else
		{
			try {
				new ServerSocket(port, 50).close();
				return true;
			} catch (Exception e) {}
		}
		return false;
	}
	/**
	 * Scan for available port till a maxOffset.
	 * @param port
	 * @param maxOffset
	 * @param host
	 * @return available port or -1.
	 */
	public static int scanAvailablePort(int port, int maxOffset, String host)
	{
		for (int offset = 0; offset < maxOffset; offset++) {
			port += offset;
			if(isPortAvailable(host, port))
				return port;
		}
		return -1;
	}
	/**
	 * Scan for available port till a maxOffset.
	 * @param port
	 * @param maxOffset
	 * @return
	 */
	public static int scanAvailablePort(int port, int maxOffset)
	{
		return scanAvailablePort(port, maxOffset, null);
	}
}
