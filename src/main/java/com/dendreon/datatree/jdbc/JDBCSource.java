package com.dendreon.datatree.jdbc;

import com.dendreon.datatree.*;
import com.dendreon.intellivenge.dataservice.DataService;
import com.dendreon.intellivenge.dataservice.DataServiceFactory;
import com.dendreon.intellivenge.dataservice.JoinParameter;
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
    private List<JoinParameter> joinParameters;
    private List<QueryParameter> staticQueryParameters;
    private List<QueryArgument> staticQueryArguments;
    private DataService dataService;
    private String[] identifyingColumns;
    private List<String> queryColumns;
    
    public JDBCSource(String table)
    {
    	this(table, null, null);
    }

    public JDBCSource(String table, String[] joinTables, JoinArgument[] joinArguments)
    {
    	this.dataService = new DataServiceFactory().createDataService();
     	this.table = table;
    	this.joinTables = joinTables;
    	this.joinArguments = joinArguments;
    	generateColumns(table);
    	if (joinTables != null) {
	    	for(String joinTable : joinTables) {
	    		generateColumns(joinTable);
	    	}
    	}
    	
    	initJoinParameters();
    }
    
    private void generateColumns(String tableName) {
    	if (columns == null) {
    		columns = new ArrayList<JDBCColumn>();
    	}

    	String baseName = "";
    	if (joinTables != null && joinTables.length > 0) {
    		baseName = tableName.toUpperCase() + ".";
    	}
    	try {
    		ResultSetMetaData tableMetaData = dataService.describeTable(tableName);
	    	for(int i = 1; i <= tableMetaData.getColumnCount(); i++) {
	    		String name = baseName + tableMetaData.getColumnName(i);
	    		Class type = javaTypeFromSqlType(tableMetaData.getColumnType(i));
	    		columns.add(new JDBCColumn(type, name));
	    		this.addQueryColumn(name);
	    	}
		} catch (SQLException e) {
			throw new RuntimeException("Couldn't index columns on table " + tableName, e);
		}
    	
	    	
    }
    
    private Class javaTypeFromSqlType(int sqlType) {
    	switch (sqlType) {
    	case java.sql.Types.CHAR:
    	case java.sql.Types.LONGVARCHAR:
    	case java.sql.Types.VARCHAR:
    		return String.class;
    	case java.sql.Types.NUMERIC:
    	case java.sql.Types.DECIMAL:
    		return BigDecimal.class;
    	case java.sql.Types.BIT:
    	case java.sql.Types.BOOLEAN:
    		return Boolean.class;
    	case java.sql.Types.TINYINT:
    		return byte.class;
    	case java.sql.Types.SMALLINT:
    		return short.class;
    	case java.sql.Types.INTEGER:
    		return int.class;
    	case java.sql.Types.BIGINT:
    		return long.class;
    	case java.sql.Types.REAL:
    		return float.class;
    	case java.sql.Types.FLOAT:
    	case java.sql.Types.DOUBLE:
    		return double.class;
    	case java.sql.Types.BINARY:
    	case java.sql.Types.VARBINARY:
    	case java.sql.Types.LONGVARBINARY:
    		return byte[].class;
    	case java.sql.Types.DATE:
    		return java.sql.Date.class;
    	case java.sql.Types.TIME:
    		return java.sql.Time.class;
    	case java.sql.Types.TIMESTAMP:
    		return java.sql.Timestamp.class;
    	case java.sql.Types.DATALINK:
    		return java.net.URL.class;
    	default:
    		return Object.class;
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
            	if (this.queryColumns == null) {
		            resultSet = dataService.findRecords(this.table, queryParameters.toArray(new QueryParameter[0]));
            	} else {
            		resultSet = dataService.findRecords(table, queryColumns.toArray(new String[0]), queryParameters)
            	}
            } else {
            	if (queryColumns == null) {
	            	resultSet = dataService.findRecords(joinParameters, queryParameters.toArray(new QueryParameter[0]));
            	} else {
	            	resultSet = dataService.findRecords(joinParameters, queryColumns.toArray(new String[0]), queryParameters.toArray(new QueryParameter[0]));
            	}
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
		if (staticQueryArguments != null) {
			for(QueryArgument staticArg : this.staticQueryArguments) {
				staticQueryParameters.add(toQueryParameter(staticArg));
			}
		}
	}
	
	private void initJoinParameters() {
		this.joinParameters = new ArrayList<JoinParameter>();
		if (joinArguments != null) {
			for(JoinArgument joinArg : joinArguments) {
				joinParameters.add(toJoinParameter(joinArg));
			}
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

	private JoinParameter toJoinParameter(JoinArgument joinArg) {
		if (joinArg != null) {
			com.dendreon.intellivenge.dataservice.JoinType joinType = com.dendreon.intellivenge.dataservice.JoinType.valueOf(com.dendreon.intellivenge.dataservice.JoinType.class, joinArg.getJoinType().name());
			return new JoinParameter(joinArg.getLeftTableName(), joinArg.getRightTableName(), joinArg.getLeftColumnName(), joinArg.getRightColumnName(), joinType);
		}
		return null;
	}
	
	public void addQueryColumn(String queryColumnName) {
		if (queryColumns == null) {
			queryColumns = new ArrayList<String>();
		}
		this.queryColumns.add(queryColumnName);
	}
}
