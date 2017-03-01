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
package org.reactivetechnologies.ticker.messaging.data.ext;

import com.hazelcast.nio.serialization.DataSerializable;
/**
 * A {@linkplain DataSerializable} which is also {@linkplain Comparable}.
 * @author esutdal
 *
 * @param <T>
 */
public interface DataComparable<T> extends Comparable<T>, DataSerializable{

	T value();
}