/*
 * Copyright (c) 2018, Ronald DeSantis
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

package com.hauldata.dbpa.file.fixed;

import java.util.LinkedList;
import java.util.List;

public class FixedFields {

	private List<FixedField> fields;

	public FixedFields() {
		fields = new LinkedList<FixedField>();
	}

	public void add(FixedField field) {
		fields.add(field);
	}

	public void actOn(String record) {
		for (FixedField field : fields) {
			field.actOn(record);
		}
	}
}
