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

package com.hauldata.dbpa.manage.sql;

public class ArgumentSql extends CommonSql {

	public static final String tableName = "JobArgument";

	public final static String createTable_ = "CREATE TABLE %1$s" + tableName + " " +
			"( jobId INTEGER, argIndex INTEGER, argName VARCHAR(255), argValue VARCHAR(255), PRIMARY KEY (jobId, argIndex) )";
	public final static String insert_ = "INSERT INTO %1$s" + tableName + " VALUES (?,?,?,?)";
	public final static String select_ = "SELECT argName, argValue FROM %1$s" + tableName + " WHERE jobId = ? ORDER BY argIndex";
	public final static String delete_ = "DELETE FROM %1$s" + tableName + " WHERE jobId = ?";
	public final static String selectAllColumns_ = "SELECT jobId, argIndex, argName, argValue FROM %1$s" + tableName;
	public final static String dropTable_ = "DROP TABLE %1$s" + tableName;

	/**
	 * Constructor initializes the following data members from the above "_" versions
	 * with the tablePrefix substituted where needed.
	 */
	public String createTable;
	public String insert;
	public String select;
	public String delete;
	public String selectAllColumns;
	public String dropTable;

	public ArgumentSql(String tablePrefix) {
		super(tablePrefix);
	}
}
