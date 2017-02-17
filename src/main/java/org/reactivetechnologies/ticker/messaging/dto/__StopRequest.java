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
package org.reactivetechnologies.ticker.messaging.dto;

import java.util.concurrent.TimeUnit;

import akka.actor.PoisonPill;

public class __StopRequest extends PoisonPill {
	public __StopRequest() {
		this(10, TimeUnit.SECONDS);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public final long timeout;
	public final TimeUnit unit;

	public __StopRequest(long timeout, TimeUnit unit) {
		super();
		this.timeout = timeout;
		this.unit = unit;
	}

}
