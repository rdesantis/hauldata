/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

public class RunSql extends CommonSql {

	public static final String tableName = "JobRun";

	public static int maxMessageLength = 255;

	public final static String createTable_ = "CREATE TABLE %1$s" + tableName + " " +
			"( id INTEGER, jobId INTEGER, jobName VARCHAR(255), status TINYINT, startTime DATETIME, endTime DATETIME, message VARCHAR(255), PRIMARY KEY (id) )";
	public final static String insert_ = "INSERT INTO %1$s" + tableName + " (id, jobId, jobName, status, message) VALUES (?,?,?,?,?)";
	public final static String selectLastId_ = "SELECT MAX(id) FROM %1$s" + tableName;
	public final static String updateById_ = "UPDATE %1$s" + tableName + " SET status = ?, startTime = ?, endTime = ?, message = ? WHERE id = ?";
	public final static String updateByStatus_ = "UPDATE %1$s" + tableName + " SET status = ?, endTime = ?, message = ? WHERE status = ?";
	public final static String selectAllColumns_ = "SELECT id, jobId, jobName, status, startTime, endTime, message FROM %1$s" + tableName;

	public final static String select_ = "SELECT id, run.jobName, status, startTime, endTime, message FROM %1$s" + tableName + " AS run";
	public final static String selectAllLastId_ = "SELECT jobName, MAX(id) AS maxId FROM %1$s" + tableName + " GROUP BY jobName";

	public String orderById = " ORDER BY id %1$s";
	public String selectLast =	"%1$s INNER JOIN (%2$s) AS mr ON mr.jobName = run.jobName AND run.id = mr.maxId";
	public String whereJobName = " WHERE run.jobName LIKE ?";

	public final static String dropTable_ = "DROP TABLE %1$s" + tableName;

	/**
	 * Constructor initializes the following data members from the above "_" versions
	 * with the tablePrefix substituted where needed.
	 */
	public String createTable;
	public String insert;
	public String selectLastId;
	public String updateById;
	public String updateByStatus;
	public String selectAllColumns;

	public String select;
	public String selectAllLastId;

	public String dropTable;

	public RunSql(String tablePrefix) {
		super(tablePrefix);
	}
}
