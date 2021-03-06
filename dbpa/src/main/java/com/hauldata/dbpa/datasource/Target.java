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

package com.hauldata.dbpa.datasource;

import java.sql.SQLException;

import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.process.Context;

public interface Target {

	void prepareStatement(Context context, Columns columns) throws SQLException;

	int getParameterCount() throws SQLException;

	int getParameterType(int parameterIndex) throws SQLException;

	void setObject(int parameterIndex, Object x) throws SQLException;

	void addBatch() throws SQLException, InterruptedException;

	public int[] executeBatch() throws SQLException, InterruptedException;

	void close(Context context);
}
