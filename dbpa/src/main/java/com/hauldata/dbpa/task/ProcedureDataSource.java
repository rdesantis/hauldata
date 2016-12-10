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

package com.hauldata.dbpa.task;

import java.sql.CallableStatement;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.expression.Reference;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.variable.VariableBase;
import com.hauldata.dbpa.variable.VariableType;

public class ProcedureDataSource extends DataSource {

	public enum ParamDirection { IN, OUT, INOUT };

	public static class DirectionalParam {
		public ExpressionBase expression;
		public ParamDirection direction;

		public DirectionalParam(ExpressionBase expression, ParamDirection direction) {
			this.expression = expression;
			this.direction = direction;
		}
	}

	private VariableBase resultParam;
	private Expression<String> procedure;
	private List<DirectionalParam> params;

	private static Map<VariableType, JDBCType> sqlTypes;
	static {
		sqlTypes = new HashMap<VariableType, JDBCType>();
		sqlTypes.put(VariableType.INTEGER, JDBCType.INTEGER);
		sqlTypes.put(VariableType.VARCHAR, JDBCType.VARCHAR);
		sqlTypes.put(VariableType.DATETIME, JDBCType.TIMESTAMP);
	}

	public ProcedureDataSource(
			DatabaseConnection connection,
			VariableBase resultParam,
			Expression<String> procedure,
			List<DirectionalParam> params) {

		super(connection);
		this.resultParam = resultParam;
		this.procedure = procedure;
		this.params = params;
	}

	@Override
	public ResultSet getResultSet(Context context) throws SQLException {

		String result = (resultParam != null) ? "? = " : "";
		String procName = procedure.evaluate();
		String paramList = params.stream().map(p -> "?").collect(Collectors.joining(","));

		String sql = "{" + result + "call " + procName + "(" + paramList + ")}";

		conn = context.getConnection(connection);

		CallableStatement thisStmt = conn.prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

		stmt = thisStmt;

		int paramIndex = 1;
		if (resultParam != null) {

			thisStmt.registerOutParameter(paramIndex, sqlTypes.get(resultParam.getType()));

			paramIndex++;
		}

		for (DirectionalParam param : params) {

			if (param.direction == ParamDirection.INOUT || param.direction == ParamDirection.OUT) {
				thisStmt.registerOutParameter(paramIndex, sqlTypes.get(param.expression.getType()));
			}

			if (param.direction == ParamDirection.IN || param.direction == ParamDirection.INOUT) {
				thisStmt.setObject(paramIndex, toSQL(param.expression.getEvaluationObject()));
			}

			paramIndex++;
		}

		rs = thisStmt.executeQuery();

		return rs;
	}

	@Override
	public void done(Context context) throws SQLException {

		CallableStatement thisStmt = (CallableStatement)stmt;

		// Get OUT parameter values.

		int paramIndex = 1;
		if (resultParam != null) {

			resultParam.setValueObject(fromSQL(thisStmt.getObject(paramIndex)));

			paramIndex++;
		}

		for (DirectionalParam param : params) {

			if (param.direction == ParamDirection.INOUT || param.direction == ParamDirection.OUT) {

				((Reference<?>)param.expression).getVariable().setValueObject(fromSQL(thisStmt.getObject(paramIndex)));
			}

			paramIndex++;
		}
	}
}
