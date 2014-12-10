package com.dendreon.datatree.jdbc;

public interface JoinArgument {
	public String getLeftTableColumn();
	
	public String getRightTableColum();
	
	public JoinType getJoinType();
}
