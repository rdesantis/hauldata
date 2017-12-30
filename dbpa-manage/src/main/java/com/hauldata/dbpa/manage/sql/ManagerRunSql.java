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

package com.hauldata.dbpa.manage.sql;

public class ManagerRunSql extends CommonSql {

	public static final String tableName = "ManagerRun";

	public final static String createTable_ = "CREATE TABLE %1$s" + tableName + " " +
			"( sequence INTEGER, startTime DATETIME, endTime DATETIME, PRIMARY KEY (sequence) )";
	public final static String insertSequence_ = "INSERT INTO %1$s" + tableName + " (sequence) VALUES (?)";
	public final static String selectLastSequence_ = "SELECT MAX(sequence) FROM %1$s" + tableName;
	public final static String updateStart_ = "UPDATE %1$s" + tableName + " SET startTime = ? WHERE sequence = ? AND startTime IS NULL";
	public final static String updateEnd_ = "UPDATE %1$s" + tableName + " SET endTime = ? WHERE sequence = ?";

	public final static String selectAllColumns_ = "SELECT sequence, startTime, endTime FROM %1$s" + tableName;
	public final static String dropTable_ = "DROP TABLE %1$s" + tableName;

	/**
	 * Constructor initializes the following data members from the above "_" versions
	 * with the tablePrefix substituted where needed.
	 */
	public String createTable;
	public String insertSequence;
	public String selectLastSequence;
	public String updateStart;
	public String updateEnd;

	public String selectAllColumns;
	public String dropTable;

	public ManagerRunSql(String tablePrefix) {
		super(tablePrefix);
	}
}
