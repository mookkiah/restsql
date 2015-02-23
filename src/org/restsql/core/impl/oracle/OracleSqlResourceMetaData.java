/* Copyright (c) restSQL Project Contributors. Licensed under MIT. */
package org.restsql.core.impl.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.restsql.core.impl.AbstractSqlResourceMetaData;
import org.restsql.core.impl.ColumnMetaDataImpl;
import org.restsql.core.sqlresource.SqlResourceDefinition;
import org.restsql.core.sqlresource.SqlResourceDefinitionUtils;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.FromBaseTable;
import com.foundationdb.sql.parser.FromList;
import com.foundationdb.sql.parser.FromTable;
import com.foundationdb.sql.parser.HalfOuterJoinNode;
import com.foundationdb.sql.parser.ResultColumnList;
import com.foundationdb.sql.parser.ResultSetNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.SelectNode;
import com.foundationdb.sql.parser.StatementNode;

/**
 * Metadata implementation for Oracle Database
 * 
 * @author Piotr Roznicki
 */

public class OracleSqlResourceMetaData extends AbstractSqlResourceMetaData {
	private static final String SQL_COLUMNS_QUERY = "select column_name, data_type, data_default from all_tab_columns where owner =  ? and table_name = ?";
	private static final String SQL_PK_QUERY = "SELECT  cols.column_name FROM all_constraints cons, all_cons_columns cols"
			+ " WHERE cons.owner = ? AND cols.table_name = ?"
			+ " AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name ";
	
	private Map<String,String> aliasToTable = new HashMap<String, String>();
	private Map<String, String> columnToTable = new HashMap<String, String>();


	/**
	 * Retrieves database name from result set meta data. Hook method for buildTablesAndColumns() allows
	 * database-specific overrides.
	 */
	@Override
	protected String getColumnDatabaseName(final SqlResourceDefinition definition,
			final ResultSetMetaData resultSetMetaData, final int colNumber) throws SQLException {
		return SqlResourceDefinitionUtils.getDefaultDatabase(definition);
	}

	/**
	 * Retrieves actual column name from result set meta data. Hook method for buildTablesAndColumns() allows
	 * database-specific overrides.
	 */
	@Override
	protected String getColumnName(final SqlResourceDefinition definition,
			final ResultSetMetaData resultSetMetaData, final int colNumber) throws SQLException {
		return ((ResultSetMetaData) resultSetMetaData).getColumnName(colNumber);
	}

	/**
	 * Retrieves table name from definition. Oracle getTableName returns null!!!
	 * overrides.
	 */
	@Override
	protected String getColumnTableName(final SqlResourceDefinition definition,
			final ResultSetMetaData resultSetMetaData, final int colNumber) throws SQLException {

		
		String columnName = resultSetMetaData.getColumnName(colNumber);
		String columnTableName = getColumnToTable().get(columnName);
		if(getAliasToTable().containsKey(columnTableName)){
			return getAliasToTable().get(columnTableName);
		}
		
		if(columnTableName == null){
			//TODO columnTableName is not idetified. Check sql parser 
			return definition.getMetadata().getTable().get(0).getName();
		}
		
		return columnTableName;

		//return ((ResultSetMetaData) resultSetMetaData).getTableName(colNumber);
	}
	

	/**
	 * Retrieves sql for querying columns. Hook method for buildInvisibleForeignKeys() and buildJoinTableMetadata()
	 * allows database-specific overrides.
	 */
	@Override
	protected String getSqlColumnsQuery() {
		return SQL_COLUMNS_QUERY;
	}

	/**
	 * Retrieves sql for querying primary keys. Hook method for buildPrimaryKeys allows database-specific overrides.
	 */
	@Override
	protected String getSqlPkQuery() {
		return SQL_PK_QUERY;
	}

	/** Retrieves database-specific table name used in SQL statements. */
	@Override
	protected String getQualifiedTableName(final SqlResourceDefinition definition,
			final ResultSetMetaData resultSetMetaData, final int colNumber) throws SQLException {
		
		String columnName = resultSetMetaData.getColumnName(colNumber);
		String columnSource = getColumnToTable().get(columnName);
		if(getAliasToTable().containsKey(columnSource)){
			return getAliasToTable().get(columnSource);
		}
		
		if(columnSource == null){
			return definition.getMetadata().getTable().get(0).getName();
		}
		
		return columnSource;
		
	}

	/** Retrieves database-specific table name used in SQL statements. Used to build join table meta data. */
	@Override
	protected String getQualifiedTableName(Connection connection, String databaseName, String tableName) {
			return tableName;
		}

	/**
	 * Return whether a column in the given result set is read-only. The Oracle implementation calls isReadOnly()
	 */
	@Override
	protected boolean isColumnReadOnly(ResultSetMetaData resultSetMetaData, int colNumber)
			throws SQLException {
		return (resultSetMetaData.isReadOnly(colNumber));
		// Hack access to protected member "fields" as we need to find the value
		// of Field.positionInTable which is 0 for columns based on SQL functions
		/*try {
			java.lang.reflect.Field fields_array = AbstractJdbc2ResultSetMetaData.class
					.getDeclaredField("fields");
			fields_array.setAccessible(true);
			final Field[] fields = (Field[]) fields_array.get(resultSetMetaData);
			return fields[colNumber - 1].getPositionInTable() == 0;
		} catch (Exception e) {
			throw new SQLException(e.toString());
		}*/
	}

	/**
	 * Sets sequence metadata for a column with the columns query result set. The column_default column will contain a
	 * string in the format nextval('sequence-name'::regclass), where sequence-name is the sequence name.
	 * 
	 * @throws SQLException when a database error occurs
	 */
	@Override
	protected void setSequenceMetaData(ColumnMetaDataImpl column, ResultSet resultSet) throws SQLException {
		final String columnDefault = resultSet.getString(3);
		if (columnDefault != null && columnDefault.startsWith("nextval")) {
			column.setSequence(true);
			column.setSequenceName(columnDefault.substring(9, columnDefault.indexOf('\'', 10)));
		}
	}

@Override
protected String getQualifiedColumnLabel(String tableName,
		String qualifiedTableName, boolean readOnly, String label) {
	return label;
}

protected boolean isDbMetaDataUpperCase() {
	return true;
}


protected void parseQuery(final SqlResourceDefinition definition){
	try{
		SQLParser parser = new SQLParser();
		StatementNode stmt = parser.parseStatement(definition.getQuery().getValue());
        CursorNode cursor = (CursorNode)stmt;
        SelectNode selectNode = (SelectNode)cursor.getResultSetNode();
        ResultColumnList rcl = selectNode.getResultColumns();
        FromList fl = selectNode.getFromList();
        for (FromTable fromTable : fl) {
        	extractCorrelations(fromTable);
		}
        
        String[] columns = rcl.getColumnNames();
        for (String columnName : columns) {
        	columnToTable.put(columnName.toUpperCase(), rcl.getResultColumn(columnName).getTableName());
		}
	}catch(Exception e){
		e.printStackTrace();
	}

}



private void extractCorrelations(FromTable fromTable)
		throws StandardException {
	if(fromTable instanceof FromBaseTable){
		FromBaseTable fbt = (FromBaseTable)fromTable;
		String originalTableName = fbt.getOrigTableName().getTableName();
		String correlationName = fbt.getCorrelationName();
		if(correlationName == null){
			correlationName = originalTableName;
		}
		aliasToTable.put(correlationName, originalTableName);
	}else if (fromTable instanceof HalfOuterJoinNode){
		HalfOuterJoinNode join = (HalfOuterJoinNode)fromTable;
		FromBaseTable fbt = ((FromBaseTable)join.getRightResultSet());
		extractCorrelations(fbt);
		ResultSetNode leftResultNode = join.getLeftResultSet();
		if(leftResultNode instanceof FromTable){
			extractCorrelations((FromTable)leftResultNode);
		}
	}else{
		System.err.println(fromTable);
	}
}


public Map<String, String> getAliasToTable() {
	return aliasToTable;
}

public Map<String, String> getColumnToTable() {
	return columnToTable;
}

}
