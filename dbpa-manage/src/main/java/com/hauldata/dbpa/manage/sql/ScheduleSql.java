/*
 * Copyright (c) 2016, 2018 Ronald DeSantis
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

public class ScheduleSql extends CommonSql {

	public static final String tableName = "Schedule";

	public final static String createTable_ = "CREATE TABLE %1$s" + tableName + " " +
			"( id INTEGER, name VARCHAR(255) UNIQUE, schedule VARCHAR(1023), PRIMARY KEY (id) )";
	public final static String insert_ = "INSERT INTO %1$s" + tableName + " VALUES (?,?,?)";
	public final static String selectLastId_ = "SELECT MAX(id) FROM %1$s" + tableName;
	public final static String selectIds_ = "SELECT id FROM %1$s" + tableName + " WHERE name IN (%%1$s)";
	public final static String select_ = "SELECT name, schedule FROM %1$s" + tableName + " WHERE name LIKE ? ORDER BY name";
	public final static String selectId_ = "SELECT id FROM %1$s" + tableName + " WHERE name = ?";
	public final static String update_ = "UPDATE %1$s" + tableName + " SET %%1$s = ? WHERE id = ?";
	public final static String delete_ = "DELETE FROM %1$s" + tableName + " WHERE id = ?";
	public final static String selectAllColumns_ = "SELECT id, name, schedule FROM %1$s" + tableName;
	public final static String selectAllColumnsByIds_ = selectAllColumns_ + " WHERE id IN (%%1$s)";
	public final static String selectNames_ = "SELECT name FROM %1$s" + tableName + " WHERE id IN (%%1$s)";
	public final static String dropTable_ = "DROP TABLE %1$s" + tableName;

	/**
	 * Constructor initializes the following data members from the above "_" versions
	 * with the tablePrefix substituted where needed.
	 */
	public String createTable;
	public String insert;
	public String selectLastId;
	public String selectIds;
	public String select;
	public String selectId;
	public String update;
	public String delete;
	public String selectAllColumns;
	public String selectAllColumnsByIds;
	public String selectNames;
	public String dropTable;

	public ScheduleSql(String tablePrefix) {
		super(tablePrefix);
	}
}
