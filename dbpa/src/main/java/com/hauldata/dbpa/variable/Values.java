/*
 * Copyright (c) 2020, Ronald DeSantis
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

package com.hauldata.dbpa.variable;

import java.util.LinkedList;
import java.util.List;

public class Values {
	private List<List<Object>> valuesLists;

	public Values() {
		valuesLists = new LinkedList<List<Object>>();
	}

	public List<List<Object>> getValuesLists() {
		return valuesLists;
	}

	public void add(List<Object> values) {
		if (!valuesLists.isEmpty() && (valuesLists.get(0).size() != values.size())) {
			throw new RuntimeException("All rows of a VALUES variables must have the same number of columns");
		}
		valuesLists.add(values);
	}

	public void clear() {
		valuesLists.clear();
	}
}
