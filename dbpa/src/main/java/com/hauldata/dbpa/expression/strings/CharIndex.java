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

package com.hauldata.dbpa.expression.strings;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.variable.VariableType;

public class CharIndex extends Expression<Integer> {

	private Expression<String> toFind;
	private Expression<String> toSearch;
	private Expression<Integer> startIndex;

	public CharIndex(
			Expression<String> toFind,
			Expression<String> toSearch,
			Expression<Integer> startIndex) {
		super(VariableType.INTEGER);
		this.toFind = toFind;
		this.toSearch = toSearch;
		this.startIndex = startIndex;
	}

	@Override
	public Integer evaluate() {

		String toFindValue = toFind.evaluate();
		String toSearchValue = toSearch.evaluate();
		Integer startIndexValue = (startIndex != null) ? startIndex.evaluate() : (Integer)1;

		if (toFindValue == null || toSearchValue == null || startIndexValue == null) {
			return null;
		}

		int fromIndex = Math.max(0, startIndexValue - 1);

		return toSearchValue.indexOf(toFindValue, fromIndex) + 1;
	}
}
