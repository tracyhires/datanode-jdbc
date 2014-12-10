package com.dendreon.datatree.jdbc;

public class JDBCColumn<T> {
	private final Class<T> type;
	private final String name;
	
	public JDBCColumn(Class<T> type, String name) {
		this.type = type;
		this.name = name;
	}
	
	public Class<T> getType() {
		return type;
	}
	
	public String getName() {
		return name;
	}
}
