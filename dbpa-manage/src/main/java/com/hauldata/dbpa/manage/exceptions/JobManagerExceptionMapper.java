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
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.hauldata.dbpa.manage.JobManagerException.BaseException;
import com.hauldata.dbpa.manage_control.api.ExceptionEntity;

@Provider
public class JobManagerExceptionMapper implements ExceptionMapper<BaseException> {

	@Override
	public Response toResponse(BaseException exception) {

		Response.Status status = null;
		switch (exception.getType()) {
		case AVAILABILITY:
			status = Response.Status.SERVICE_UNAVAILABLE;
			break;
		case CONFLICT:
			status = Response.Status.CONFLICT;
			break;
		}
		return ExceptionEntity.response(status, exception);
	}
}
