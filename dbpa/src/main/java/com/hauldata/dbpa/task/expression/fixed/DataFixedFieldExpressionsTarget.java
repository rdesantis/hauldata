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

package com.hauldata.dbpa.task.expression.fixed;

import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.file.fixed.DataFixedFieldsTarget;

public class DataFixedFieldExpressionsTarget {

	private List<FixedFieldExpression> fields;
	private DataTarget target;

	public DataFixedFieldExpressionsTarget() {
		fields = new LinkedList<FixedFieldExpression>();
	}

	public void add(FixedFieldExpression field) {
		fields.add(field);
	}

	public void setTarget(DataTarget target) {
		this.target = target;
	}

	public boolean hasJoin() {
		return fields.stream().anyMatch(field -> field instanceof KeeperFixedFieldExpression && ((KeeperFixedFieldExpression)field).isJoined());
	}

	public boolean hasValidator() {
		return fields.stream().anyMatch(field -> field instanceof ValidatorFixedFieldExpression);
	}

	public DataFixedFieldsTarget evaluate() {
		DataFixedFieldsTarget result = new DataFixedFieldsTarget();
		for (FixedFieldExpression field : fields) {
			result.add(field.evaluate());
		}
		result.setTarget(target);
		return result;
	}
}
