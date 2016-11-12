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

public class JobScheduleSql extends CommonSql {

	public static final String tableName = "JobSchedule";

	public final static String createTable_ = "CREATE TABLE %1$s" + tableName + " " +
			"( jobId INTEGER, scheduleId INTEGER, PRIMARY KEY (jobId, scheduleId) )";
	public final static String insert_ = "INSERT INTO %1$s" + tableName + " VALUES (?,?)";
	public final static String selectScheduleNamesByJobId_ =
			"SELECT s.name " +
			"FROM %1$s" + ScheduleSql.tableName + " AS s " +
			"INNER JOIN %1$s" + tableName + " AS js ON s.id = js.scheduleId " +
			"WHERE js.jobId = ?";
	public final static String selectEnabledJobSchedules_ =
			"SELECT s.id, s.name, s.schedule " +
			"FROM %1$s" + ScheduleSql.tableName + " AS s " +
			"INNER JOIN %1$s" + tableName + " AS js ON s.id = js.scheduleId " +
			"INNER JOIN %1$s" + JobSql.tableName + " AS j ON js.jobId = j.id " +
			"WHERE j.enabled = 1 GROUP BY s.id, s.name, s.schedule";
	public final static String delete_ = "DELETE FROM %1$s" + tableName + " WHERE jobId = ?";
	public final static String selectJobNamesByScheduleId_ =
			"SELECT j.name " +
			"FROM %1$s" + JobSql.tableName + " AS j " +
			"INNER JOIN %1$s" + tableName + " AS js ON j.id = js.jobId " +
			"WHERE js.scheduleId = ?";
	public final static String selectEnabledJobNamesByScheduleId_ = selectJobNamesByScheduleId_ + " AND j.enabled = 1";
	public final static String selectAllColumns_ = "SELECT jobId, scheduleId FROM %1$s" + tableName;
	public final static String dropTable_ = "DROP TABLE %1$s" + tableName;

	/**
	 * Constructor initializes the following data members from the above "_" versions
	 * with the tablePrefix substituted where needed.
	 */
	public String createTable;
	public String insert;
	public String selectScheduleNamesByJobId;
	public String selectEnabledJobSchedules;
	public String delete;
	public String selectJobNamesByScheduleId;
	public String selectEnabledJobNamesByScheduleId;
	public String selectAllColumns;
	public String dropTable;

	public JobScheduleSql(String tablePrefix) {
		super(tablePrefix);
	}
}
