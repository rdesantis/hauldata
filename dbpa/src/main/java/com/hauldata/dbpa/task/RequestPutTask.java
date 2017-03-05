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

package com.hauldata.dbpa.task;

import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;

import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.datasource.Source;

public class RequestPutTask extends RequestWithBodyTask {

	public RequestPutTask(
			Prologue prologue,
			Source source,
			Parameters parameters,
			DataTarget target) {
		super(prologue, source, parameters, target);
	}

	@Override
	protected HttpRequestBase makeRequest() {
		return new HttpPut();
	}
}
