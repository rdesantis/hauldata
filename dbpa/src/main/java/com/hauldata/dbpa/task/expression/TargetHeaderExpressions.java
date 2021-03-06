/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

package com.hauldata.dbpa.task.expression;

import java.util.ArrayList;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.TargetHeaders;

public class TargetHeaderExpressions extends HeaderExpressions {

	protected boolean fromMetadata;

	public TargetHeaderExpressions(
			boolean exist,
			boolean fromMetadata,
			ArrayList<Expression<String>> captions) {

		super(exist, captions);

		this.fromMetadata = fromMetadata;
	}

	public TargetHeaderExpressions() {
		this(false, false, new ArrayList<Expression<String>>());
	}

	public boolean fromMetadata() {
		return fromMetadata;
	}

	public TargetHeaders evaluate() {
		return new TargetHeaders(exist, fromMetadata, evaluateCaptions());
	}
}
