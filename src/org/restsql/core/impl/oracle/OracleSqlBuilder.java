/* Copyright (c) restSQL Project Contributors. Licensed under MIT. */
package org.restsql.core.impl.oracle;

import org.restsql.core.impl.AbstractSqlBuilder;

/**
 * Adds limit clause.
 * 
 * @author Mark Sawers
 */
public class OracleSqlBuilder extends AbstractSqlBuilder {

	@Override
	public String buildSelectLimitSql(final int limit, final int offset) {
		StringBuilder string = new StringBuilder(25);
		string.append(" OFFSET ");
		string.append(offset);
		string.append(" ROWS FETCH NEXT ");
		string.append(limit);
		string.append(" ROWS ONLY");
		
		return string.toString(); //TODO
	}

}
