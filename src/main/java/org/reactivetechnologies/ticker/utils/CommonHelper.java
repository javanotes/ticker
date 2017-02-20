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

import org.reactivetechnologies.ticker.messaging.Data;

import com.hazelcast.internal.serialization.impl.JavaDefaultSerializers.JavaSerializer;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.ObjectDataOutput;

public class CommonHelper {
	private CommonHelper() {
	}

	private static JavaSerializer javaSerializer;
	static
	{
		javaSerializer = new JavaSerializer(true, false);
	}
	public static Data deepCopy(Data orig) throws IOException
	{
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		ObjectDataOutput out = new ObjectDataOutputStream(new ByteArrayOutputStream(), null);
		try 
		{
			javaSerializer.write(out, orig);
			ObjectDataInputStream in = new ObjectDataInputStream(new ByteArrayInputStream(bytes.toByteArray()), null);
			return (Data) javaSerializer.read(in);
		} catch (IOException e) {
			throw e;
		}
	}
}
