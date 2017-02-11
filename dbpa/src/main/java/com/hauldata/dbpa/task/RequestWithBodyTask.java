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

import com.hauldata.dbpa.expression.Expression;

public abstract class RequestWithBodyTask extends RequestTask {

	protected List<Expression<String>> bodyFields;

	public static class ParametersWithBody extends Parameters {

		public List<Expression<String>> bodyFields;
	}

	public RequestWithBodyTask(Prologue prologue, ParametersWithBody parameters) {
		super(prologue, parameters);

		bodyFields = parameters.bodyFields;
	}
}
