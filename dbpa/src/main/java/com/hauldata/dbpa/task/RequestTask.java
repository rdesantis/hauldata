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

import java.util.List;

import com.hauldata.dbpa.datasource.DataSource;
import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.expression.Expression;

public abstract class RequestTask extends Task {

	protected Expression<String> url;
	protected Expression<String> header;

	protected List<Expression<String>> requestFields;
	protected DataSource source;
	protected List<Expression<String>> keepFields;
	protected List<Expression<String>> responseFields;
	protected Expression<String> statusField;
	protected DataTarget target;

	public static class Parameters {

		public Expression<String> url;
		public Expression<String> header;

		public List<Expression<String>> requestFields;
		public DataSource source;
		public List<Expression<String>> keepFields;
		public List<Expression<String>> responseFields;
		public Expression<String> statusField;
		public DataTarget target;
	}

	protected RequestTask(Prologue prologue, Parameters parameters) {
		super(prologue);
		
		url = parameters.url;
		header = parameters.header;

		requestFields = parameters.requestFields;
		source = parameters.source;
		keepFields = parameters.keepFields;
		responseFields = parameters.responseFields;
		statusField = parameters.statusField;
		target = parameters.target;
	}
}
