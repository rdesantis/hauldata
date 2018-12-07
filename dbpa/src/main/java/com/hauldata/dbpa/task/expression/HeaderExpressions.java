/*
 * Copyright (c) 2016, Ronald DeSantis
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

public class HeaderExpressions {
	protected boolean exist;
	protected ArrayList<Expression<String>> captions;

	protected HeaderExpressions(
			boolean exist,
			ArrayList<Expression<String>> captions) {

		this.exist = exist;
		this.captions = captions;
	}

	public boolean exist() {
		return exist;
	}

	public ArrayList<Expression<String>> getCaptions() {
		return captions;
	}

	protected ArrayList<String> evaluateCaptions() {

		ArrayList<String> evaluatedCaptions = new ArrayList<String>();
		for (Expression<String> caption : captions) {
			evaluatedCaptions.add(caption.evaluate());
		}
		return evaluatedCaptions;
	}
}

