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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;

import javax.naming.NameNotFoundException;

public class CommonSql {

	/**
	 * Use reflection to initialize data members from like-named members with "_" suffix
	 * substituting tablePrefix where "%1$s" appears in the "_" member. 
	 * @param tablePrefix
	 */
	protected CommonSql(String tablePrefix) {

		Iterator<String> nameIterator = Arrays.stream(this.getClass().getDeclaredFields()).map(f -> f.getName()).filter(n -> n.endsWith("_")).iterator();

		while (nameIterator.hasNext()) {
			try {
				String name_ = nameIterator.next();
				String name = name_.substring(0, name_.length() - 1);

				Field field_ = this.getClass().getDeclaredField(name_);
				Field field = this.getClass().getDeclaredField(name);

				String value_ = (String)field_.get(this);

				String value = String.format(value_, tablePrefix);

				field.set(this, value);
			}
			catch (Exception ex) {}
		}
	}

	public static int getNextId(Connection conn, String selectLastId) throws Exception {

		int nextId = -1;

		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			rs = stmt.executeQuery(selectLastId);

			rs.next();

			nextId = rs.getInt(1) + 1;
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}

		return nextId;
	}

	public static int getId(Connection conn, String selectId, String name, String entityName) throws Exception {

		int id = -1;

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(selectId, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setString(1, name);

			rs = stmt.executeQuery();

			if (!rs.next()) {
				throw new NameNotFoundException(entityName + " not found");
			};

			id = rs.getInt(1);
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}

		return id;
	}
}
