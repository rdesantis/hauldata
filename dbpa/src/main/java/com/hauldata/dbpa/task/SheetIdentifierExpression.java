/*
 * Copyright (c) 2016, 2018, Ronald DeSantis
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

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.FileHandler;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.book.SheetIdentifier;
import com.hauldata.dbpa.process.Context;

public class SheetIdentifierExpression extends PhysicalPageIdentifierExpression {

	protected Expression<String> sheetName;

	public SheetIdentifierExpression(FileHandler handler, Expression<String> filePath, Expression<String> sheetName) {

		super(handler, filePath);
		this.sheetName = sheetName;
	}

	@Override
	public PageIdentifier evaluate(Context context, boolean writeNotRead) {
		return new SheetIdentifier(handler, context.getDataPath(getEvaluatedFilePath(), writeNotRead), getEvaluatedSheetName());
	}

	private String getEvaluatedSheetName() {
		String evaluatedSheetName = sheetName.evaluate();
		if (evaluatedSheetName == null) {
			throw new RuntimeException("Sheet name expression evaluates to NULL");
		}
		return evaluatedSheetName;
	}
}
