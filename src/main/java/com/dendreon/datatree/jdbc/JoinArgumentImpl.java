package com.dendreon.datatree.jdbc;

public class JoinArgumentImpl implements JoinArgument {
	
	private final String leftTableName;
	private final String leftColumnName;
	private final String rightTableName;
	private final String rightColumnName;
	private final JoinType joinType;
	
	public JoinArgumentImpl(String leftTableName, String leftColumnName, String rightTableName, String rightColumnName, JoinType joinType) {
		this.leftTableName = leftTableName;
		this.leftColumnName = leftColumnName;
		this.rightTableName = rightTableName;
		this.rightColumnName = rightColumnName;
		this.joinType = joinType;
	}

	@Override
	public String getLeftTableName() {
		return leftTableName;
	}
		
	@Override
	public String getLeftColumnName() {
		return leftColumnName;
	}

	@Override
	public String getRightTableName() {
		return rightTableName;
	}

	@Override
	public String getRightColumnName() {
		return rightColumnName;
	}

	@Override
	public JoinType getJoinType() {
		return joinType;
	}

}
