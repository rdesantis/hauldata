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

package com.hauldata.dbpa.datasource;

import java.sql.SQLException;

import com.hauldata.dbpa.process.Context;

public interface Source {

	boolean hasMetadata();

	void executeQuery(Context context) throws SQLException, InterruptedException;

	int getColumnCount() throws SQLException;

	String getColumnLabel(int column) throws SQLException;

	boolean next() throws SQLException, InterruptedException;

	Object getObject(int columnIndex) throws SQLException;

	boolean isLast() throws SQLException;

	void done(Context context) throws SQLException;

	void close(Context context);
}
