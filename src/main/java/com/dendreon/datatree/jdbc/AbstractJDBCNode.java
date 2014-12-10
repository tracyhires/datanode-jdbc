package com.dendreon.datatree.jdbc;

import com.dendreon.datatree.DataNode;
import com.dendreon.datatree.DataSource;
import com.dendreon.datatree.DataTree;
import com.dendreon.datatree.OrderArgument;
import com.dendreon.datatree.PropertyMapping;
import com.dendreon.datatree.impl.DataNodeImpl;
import com.dendreon.datatree.impl.OrderArgumentImpl;
import com.dendreon.datatree.impl.OrderImpl;
import com.dendreon.datatree.impl.StandardPropertyMapping;
import com.dendreon.datatree.propertymapping.BigDecimalToBooleanPropertyMapping;
import com.dendreon.datatree.propertymapping.ByteToBooleanPropertyMapping;
import com.dendreon.datatree.propertymapping.IntegerToBooleanPropertyMapping;
import com.dendreon.datatree.propertymapping.LongToBooleanPropertyMapping;
import com.dendreon.datatree.sources.CachedDataSource;

import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractJDBCNode extends DataNodeImpl {
	private final String tableName;
	private Map<String, Class> columnTypes;

	public AbstractJDBCNode(String id, DataTree tree, String table) {
		this(id, id, tree, table);
	}

	public AbstractJDBCNode(String id, DataNode parent, String table) {
		this(id, id, parent, table);
	}

	public AbstractJDBCNode(String id, DataTree tree, String table, String[] joinTables, JoinArgument[] joinClause) {
		this(id, id, tree, table, joinTables, joinClause);
	}

	public AbstractJDBCNode(String id, DataNode parent, String table, String[] joinTables, JoinArgument[] joinClause) {
		this(id, id, parent, table, joinTables, joinClause);
	}

	public AbstractJDBCNode(String id, String alias, DataTree tree, String table) {
		this(id, alias, tree, table, null, null);
	}

	public AbstractJDBCNode(String id, String alias, DataNode parent, String table) {
		this(id, alias, parent, table, null, null);
	}

	public AbstractJDBCNode(String id, String alias, DataTree tree, String table, String[] joinTables, JoinArgument[] joinClause) {
		super(id, alias, tree, createDataSource(table, joinTables, joinClause));
		this.tableName = table;
		setReadOnly(true);
		setResponsive(false);
		setup();
	}

	public AbstractJDBCNode(String id, String alias, DataNode parent, String table, String[] joinTables, JoinArgument[] joinClause) {
		super(id, alias, parent, createDataSource(table, joinTables, joinClause));
		this.tableName = table;
		setReadOnly(true);
		setResponsive(false);
		setup();
	}

	public JDBCSource getInputSource() {
		CachedDataSource cachedDataSource = (CachedDataSource) getDataSource();
		return (JDBCSource) cachedDataSource.getWrappedInputSource();
	}

	protected static DataSource createDataSource(String tableName, String[] joinTables, JoinArgument[] joinClause) {
		JDBCSource inputSource = new JDBCSource(tableName, joinTables, joinClause);
		CachedDataSource cachedDataSource = new CachedDataSource(inputSource);
		return cachedDataSource;
	}

	protected abstract void setup();

	protected PropertyMapping[] createStandardPropertyMappings(String[] propertyIds, String[] columnNames) {
		if (propertyIds.length != columnNames.length) {
			throw new RuntimeException(
					"called createStandardPropertyMappings(String[], String[]) with a different number of property ids than column names");
		}

		PropertyMapping[] vRetVal = new PropertyMapping[propertyIds.length];

		for (int i = 0; i < propertyIds.length; i++) {
			vRetVal[i] = createStandardPropertyMapping(propertyIds[i], columnNames[i]);
		}

		return vRetVal;
	}

	protected PropertyMapping createStandardPropertyMapping(String propertyId, String columnName) {
		return createStandardPropertyMapping(propertyId, columnName, true);
	}

	protected PropertyMapping createStandardPropertyMapping(String propertyId, String columnName, boolean isVisible) {
		return createStandardPropertyMapping(propertyId, columnName, getColumnType(columnName), isVisible);
	}

	protected PropertyMapping createStandardPropertyMapping(String propertyId, String columnName, Class propertyType,
			boolean isVisible) {
		StandardPropertyMapping mapping = new StandardPropertyMapping(propertyId, columnName, propertyType);
		addPropertyMapping(mapping, isVisible);

		return mapping;
	}

	protected PropertyMapping createBooleanPropertyMapping(String propertyId, String columnName, Number trueValue) {
		return createBooleanPropertyMapping(propertyId, columnName, trueValue, true);
	}

	protected PropertyMapping createBooleanPropertyMapping(String propertyId, String columnName, Number trueValue,
			boolean isVisible) {
		PropertyMapping mapping;
		if (trueValue instanceof BigDecimal) {
			mapping = new BigDecimalToBooleanPropertyMapping(propertyId, columnName, (BigDecimal) trueValue);
		} else if (trueValue instanceof Byte) {
			mapping = new ByteToBooleanPropertyMapping(propertyId, columnName, (Byte) trueValue);
		} else if (trueValue instanceof Long) {
			mapping = new LongToBooleanPropertyMapping(propertyId, columnName, trueValue.longValue());
		} else {
			mapping = new IntegerToBooleanPropertyMapping(propertyId, columnName, trueValue.intValue());
		}
		addPropertyMapping(mapping, isVisible);
		return mapping;
	}

	protected void initColumnTypes() {
		DataSource dataSource = getDataSource();
		columnTypes = new HashMap<String, Class>();
		for (int i = 0; i < dataSource.getColumnCount(); i++) {
			LoggerFactory.getLogger(getClass()).debug(
					"Column [" + dataSource.getColumnName(i) + "] has type [" + dataSource.getColumnType(i).getSimpleName()
							+ "]");
			columnTypes.put(dataSource.getColumnName(i), dataSource.getColumnType(i));
		}
	}

	protected Class getColumnType(String aColumnName) {
		if (columnTypes == null) {
			initColumnTypes();
		}

		Class vColumnType = columnTypes.get(aColumnName);

		if (vColumnType == null) {
			LoggerFactory.getLogger(getClass()).warn("Did not find column type for column named [" + aColumnName + "]");
		}

		return vColumnType;
	}

	public void setSortOrder(String... propertyIds) {
		OrderArgument[] orderArgs = new OrderArgument[propertyIds.length];
		for (int i = 0; i < propertyIds.length; i++) {
			orderArgs[i] = new OrderArgumentImpl(propertyIds[i]);
		}
		setSortOrder(orderArgs);
	}

	public void setSortOrder(OrderArgument... properties) {
		OrderImpl order = new OrderImpl();
		for (OrderArgument orderArgument : properties) {
			order.addArgument(orderArgument);
		}
		setSortOrder(order);
	}
}
