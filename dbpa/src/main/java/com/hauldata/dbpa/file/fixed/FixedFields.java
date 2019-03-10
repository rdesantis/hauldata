/*
 * Copyright (c) 2018, 2019, Ronald DeSantis
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
	private List<ValidatorFixedField> validatorFields;
	private List<FixedField> nonValidatorFields;

	public FixedFields() {
		fields = new LinkedList<FixedField>();
		validatorFields = new LinkedList<ValidatorFixedField>();
		nonValidatorFields = new LinkedList<FixedField>();
	}

	public void add(FixedField field) {
		fields.add(field);
		if (field instanceof ValidatorFixedField) {
			validatorFields.add((ValidatorFixedField)field);
		}
		else {
			nonValidatorFields.add(field);
		}
	}

	public boolean matches(String record) {
		return validatorFields.isEmpty() || validatorFields.stream().allMatch(field -> field.isExpectedIn(record));
	}

	public void actOn(int lineNumber, String record) {
		for (FixedField field : fields) {
			field.actOn(lineNumber, record);
		}
	}

	public void actNonMatchersOn(int lineNumber, String record) {
		for (FixedField field : nonValidatorFields) {
			field.actOn(lineNumber, record);
		}
	}
}
