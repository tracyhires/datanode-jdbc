package com.dendreon.datatree.jdbc;

public interface JoinArgument {
	public String getLeftTableName();
	
	public String getLeftColumnName();
	
	public String getRightTableName();
	
	public String getRightColumnName();
	
	public JoinType getJoinType();
}
