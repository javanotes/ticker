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
package org.reactivetechnologies.ticker.rest;

import org.restexpress.Request;
import org.restexpress.Response;
/**
 * Interface for HttpServlet like methods.
 * @author esutdal
 *
 */
public interface RestHandler {

	/**
	 * The url mapping
	 * @return
	 */
	String url();
	/**
	 * 	
	 * @param request
	 * @param response
	 * @throws Exception
	 */
	void create(Request request, Response response) throws Exception;

	/**
	 * 
	 * @param request
	 * @param response
	 * @throws Exception
	 */
	void read(Request request, Response response) throws Exception;

	/**
	 * 
	 * @param request
	 * @param response
	 * @throws Exception
	 */
	void update(Request request, Response response) throws Exception;

	/**
	 * 
	 * @param request
	 * @param response
	 * @throws Exception
	 */
	void delete(Request request, Response response) throws Exception;

}