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

package com.hauldata.dbpa.datasource;

import java.sql.CallableStatement;
import java.sql.Types;
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

	private static Map<VariableType, Integer> sqlTypes;
	static {
		sqlTypes = new HashMap<VariableType, Integer>();
		sqlTypes.put(VariableType.INTEGER, Types.INTEGER);
		sqlTypes.put(VariableType.VARCHAR, Types.VARCHAR);
		sqlTypes.put(VariableType.DATETIME, Types.TIMESTAMP);
	}

	public ProcedureDataSource(
			DatabaseConnection connection,
			VariableBase resultParam,
			Expression<String> procedure,
			List<DirectionalParam> params,
			boolean singleRow) {

		super(connection, singleRow);
		this.resultParam = resultParam;
		this.procedure = procedure;
		this.params = params;
	}

	@Override
	public void executeUpdate(Context context) throws SQLException, InterruptedException {

		prepareCall(context);

		executePreparedUpdate();
	}

	@Override
	public ResultSet executeQuery(Context context) throws SQLException, InterruptedException {

		prepareCall(context);

		rs = executePreparedQuery();

		return rs;
	}

	private void prepareCall(Context context) throws SQLException {

		String result = (resultParam != null) ? "? = " : "";
		String procName = procedure.evaluate();
		String paramList = params.stream().map(p -> "?").collect(Collectors.joining(","));

		String sql = "{" + result + "call " + procName + "(" + paramList + ")}";

		conn = context.getConnection(connection);

		stmt = conn.prepareCall(sql, getResultSetType(), ResultSet.CONCUR_READ_ONLY);

		CallableStatement callable = (CallableStatement)stmt;

		int paramIndex = 1;
		if (resultParam != null) {

			callable.registerOutParameter(paramIndex, sqlTypes.get(resultParam.getType()));

			paramIndex++;
		}

		for (DirectionalParam param : params) {

			if (param.direction == ParamDirection.INOUT || param.direction == ParamDirection.OUT) {
				callable.registerOutParameter(paramIndex, sqlTypes.get(param.expression.getType()));
			}

			if (param.direction == ParamDirection.IN || param.direction == ParamDirection.INOUT) {
				callable.setObject(paramIndex, toSQL(param.expression.getEvaluationObject()));
			}

			paramIndex++;
		}
	}

	@Override
	public void done(Context context) throws SQLException {

		CallableStatement callable = (CallableStatement)stmt;

		// Get OUT parameter values.

		int paramIndex = 1;
		if (resultParam != null) {

			resultParam.setValueObject(fromSQL(callable.getObject(paramIndex)));

			paramIndex++;
		}

		for (DirectionalParam param : params) {

			if (param.direction == ParamDirection.INOUT || param.direction == ParamDirection.OUT) {

				((Reference<?>)param.expression).getVariable().setValueObject(fromSQL(callable.getObject(paramIndex)));
			}

			paramIndex++;
		}
	}
}
