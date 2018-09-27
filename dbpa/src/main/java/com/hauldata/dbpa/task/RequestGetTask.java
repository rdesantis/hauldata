/*
 * Copyright (c) 2017, 2018, Ronald DeSantis
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

import java.util.List;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;

import com.hauldata.dbpa.expression.Expression;

public class RequestGetTask extends RequestTask {

	public RequestGetTask(
			Prologue prologue,
			Expression<String> url,
			Expression<Integer> connectTimeout,
			Expression<Integer> socketTimeout,
			List<Header> headers,
			List<SourceWithAliases> sourcesWithAliases,
			Expression<String> responseTemplate,
			List<TargetWithKeepers> targetsWithIdentifiers) {
		super(prologue, url, connectTimeout, socketTimeout, headers, sourcesWithAliases, responseTemplate, targetsWithIdentifiers);
	}

	@Override
	protected HttpRequestBase makeRequest() {
		return new HttpGet();
	}
}
