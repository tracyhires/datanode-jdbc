package com.dendreon.datatree.jdbc;

import com.dendreon.datatree.*;
import com.dendreon.intellivenge.dataservice.DataService;
import com.dendreon.intellivenge.dataservice.QueryParameter;
import com.dendreon.intellivenge.dataservice.QueryType;

import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JDBCSource implements InputSource, QueryableInputSource, OutputSource
{
    private final String table;
    private final String[] joinTables;
    private final JoinArgument[] joinArguments;
    private List<JDBCColumn> columns;
    private List<QueryParameter> joinParameters;
    private List<QueryParameter> staticQueryParameters;
    private List<QueryArgument> staticQueryArguments;
    private DataService dataService;
    private String[] identifyingColumns;
    
    public JDBCSource(String table)
    {
    	this(table, null, null);
    }

    public JDBCSource(String table, String[] joinTables, JoinArgument[] joinArguments)
    {
    	this.table = table;
    	this.joinTables = joinTables;
    	this.joinArguments = joinArguments;
    	generateColumns(table);
    	for(String joinTable : joinTables) {
    		generateColumns(joinTable);
    	}
    	initJoinQueryParameters();
    }
    
    private void generateColumns(String tableName) {
    	if (columns == null) {
    		columns = new ArrayList<JDBCColumn>();
    	}

    	String baseName = "";
    	if (joinTables != null && joinTables.length > 0) {
    		baseName = tableName + ".";
    	}
    	try {
    		ResultSetMetaData tableMetaData = dataService.describeTable(tableName);
	    	for(int i = 1; i <= tableMetaData.getColumnCount(); i++) {
	    		String name = baseName + tableMetaData.getColumnName(i);
	    		Class type = javaTypeFromSqlType(tableMetaData.getColumnType(i));
	    		columns.add(new JDBCColumn(type, name));
	    	}
		} catch (SQLException e) {
			throw new RuntimeException("Couldn't index columns on table " + tableName, e);
		}
    	
	    	
    }
    
    private Class javaTypeFromSqlType(int sqlType) {
    	switch (sqlType) {
    	case java.sql.Types.BIT:
    	case java.sql.Types.TINYINT:
    		return Byte.class;
    	case java.sql.Types.SMALLINT:
    	case java.sql.Types.INTEGER:
    		return Integer.class;
    	case java.sql.Types.BIGINT:
    		return Long.class;
    	case java.sql.Types.FLOAT:
    		return Float.class;
    	case java.sql.Types.REAL:
    	case java.sql.Types.DOUBLE:
    		return Double.class;
    	case java.sql.Types.DECIMAL:
    		return BigDecimal.class;
    	case java.sql.Types.CHAR:
    		return char.class;
    	case java.sql.Types.VARCHAR:
    	case java.sql.Types.LONGVARCHAR:
    	case java.sql.Types.NCHAR:
    	case java.sql.Types.NVARCHAR:
    	case java.sql.Types.LONGNVARCHAR:
    		return String.class;
    	case java.sql.Types.DATE:
    		return java.sql.Date.class;
    	case java.sql.Types.TIME:
    		return java.sql.Time.class;
    	case java.sql.Types.TIMESTAMP:
    		return java.sql.Timestamp.class;
    	case java.sql.Types.BOOLEAN:
    		return Boolean.class;
    	case java.sql.Types.BINARY:
    	case java.sql.Types.VARBINARY:
    	case java.sql.Types.LONGVARBINARY:
    	case java.sql.Types.BLOB:
    	case java.sql.Types.CLOB:
    	case java.sql.Types.ARRAY:
    	case java.sql.Types.NCLOB:
    		return Array.class;
    		default:
    			return null;
    	}
    }

    public void addStaticQueryArgument(QueryArgument argument)
    {
        if (staticQueryArguments == null)
        {
            staticQueryArguments = new ArrayList<QueryArgument>();
        }

        staticQueryArguments.add(argument);
        staticQueryParameters = null;
    }

    public Iterator<InputSourceRecord> load()
    {
        return load(null);
    }

    public Iterator<InputSourceRecord> load(Query query)
    {
        try
        {
        	List<QueryParameter> queryParameters = new ArrayList<QueryParameter>();
        	if (staticQueryParameters == null) {
        		initStaticQueryParameters();
        	}
        	queryParameters.addAll(staticQueryParameters);

            if (query != null)
            {
                for (int i=0;i<query.getArgumentCount();i++)
                {
                    QueryArgument argument = query.getArgument(i);
                    queryParameters.add(toQueryParameter(argument));
                }
            }
            
            
            ResultSet resultSet = null;
            if (joinTables == null || joinTables.length == 0) {
	            resultSet = dataService.findRecords(this.table, queryParameters.toArray(new QueryParameter[0]));
            } else {
            	String[] tableNames = new String[joinTables.length+1];
            	tableNames[0] = table;
            	for(int i = 0; i < joinTables.length; i++) {
            		tableNames[i+1] = joinTables[i];
            	}
//            	resultSet = dataService.findRecords(tableNames, joinParameters.toArray(new QueryParameter[0]), queryParameters.toArray(new QueryParameter[0]));
            }
            return new JDBCIterator(resultSet);
        }
        catch (Exception exc)
        {
            throw new RuntimeException(exc);
        }
    }
    
    public Iterator<InputSourceRecord> getInputRecords()
    {
        return load();
    }

    public Iterator<InputSourceRecord> getInputRecords(Query query)
    {
        return load(query);
    }

    public String[] getIdentifyingColumns()
    {
        return identifyingColumns;
    }
    
	@Override
	public void setSortOrder(String[] columnIds, OrderType[] orderDirection) {
		//not implemented
	}
	
	@Override
	public boolean isDirty() {
		return false;
	}

	@Override
	public void save() {
		//Nothing to do.  We immediately save records
	}

	@Override
	public int getColumnCount() {
		if (columns == null) {
			return 0;
		}
		return columns.size();
	}

	@Override
	public Class getColumnType(int aColumn) {
		if (aColumn < getColumnCount()) {
			return columns.get(aColumn).getType();
		}
		return null;
	}

	@Override
	public String getColumnName(int aColumn) {
		if (aColumn < getColumnCount()) {
			return columns.get(aColumn).getName();
		}
		return null;
	}
	
	private void initStaticQueryParameters() {
		this.staticQueryParameters = new ArrayList<QueryParameter>();
		for(QueryArgument staticArg : this.staticQueryArguments) {
			staticQueryParameters.add(toQueryParameter(staticArg));
		}
	}
	
	private void initJoinQueryParameters() {
		this.joinParameters = new ArrayList<QueryParameter>();
		for(JoinArgument joinArg : joinArguments) {
			joinParameters.add(toQueryParameter(joinArg));
		}
	}
	
	private QueryParameter toQueryParameter(QueryArgument queryArg) {
		if (queryArg != null) {
			String columnName = queryArg.getColumn();
			QueryType queryType = null;
			Object comparisonValue = queryArg.getComparisonValue();
			switch(queryArg.getComparisonType()) {
			case CONTAINS:
				queryType = QueryType.CONTAINS;
				break;
			case EQUALS:
				queryType = QueryType.EQ;
				break;
			case IN:
				queryType = QueryType.IN;
				break;
			case LESS_THAN:
				queryType = QueryType.LT;
				break;
			case GREATER_THAN:
				queryType = QueryType.GT;
				break;
			case LESS_THAN_OR_EQUAL:
				queryType = QueryType.LTE;
				break;
			case GREATER_THAN_OR_EQUAL:
				queryType = QueryType.GTE;
				break;
			}
			return new QueryParameter(columnName, queryType, comparisonValue);
		}
		return null;
	}

	private QueryParameter toQueryParameter(JoinArgument joinArg) {
		if (joinArg != null) {
			String leftColumnName = joinArg.getLeftTableColumn();
			String rightColumnName = joinArg.getRightTableColum();
			QueryType queryType = null;
			switch(joinArg.getJoinType()) {
			case INNER_JOIN:
				queryType = QueryType.INNER_JOIN;
				break;
			case OUTER_JOIN:
				queryType = QueryType.OUTER_JOIN;
				break;
			}
			return new QueryParameter(leftColumnName, queryType, rightColumnName);
		}
		return null;
	}
}
