/*
 * Copyright (c) 2017, Ronald DeSantis
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */

package com.hauldata.dbpa.manage.exceptions;

import javax.ws.rs.core.Response;

/**
 * This is not a true exception mapper.  It is used in the implementation of other mappers.  Do not register it. 
 */
public class NotFoundExceptionMapper {

	public static Response toResponse(Exception exception) {
		return Response.status(Response.Status.NOT_FOUND).entity(exception.getMessage()).build();
	}
}
