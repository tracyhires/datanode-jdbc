package com.dendreon.datatree.propertymapping;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;

public class ConcatenatedPropertyMapping<COLUMN_TYPE, SECONDARY_COLUMN_TYPE> implements PropertyMapping<String, COLUMN_TYPE> {

	private final String propertyId;
	private final String primaryColumnId;
	private final String secondaryColumnId;

	public ConcatenatedPropertyMapping(String propertyId, String primaryColumnId, String secondaryColumnId)
	{
		this.propertyId = propertyId;
		this.primaryColumnId = primaryColumnId;
		this.secondaryColumnId = secondaryColumnId;
	}

	@Override
	public String getPropertyId() {
		return propertyId;
	}

	@Override
	public Class getPropertyType() {
		return String.class;
	}

	@Override
	public String getPropertyValue(InputSourceRecord record) {
		return concatenate((COLUMN_TYPE)record.getValue(primaryColumnId), (SECONDARY_COLUMN_TYPE)record.getValue(secondaryColumnId));
	}

	@Override
	public String getColumnName() {
		return primaryColumnId;
	}

	@Override
	public COLUMN_TYPE toColumnType(String propertyValue) {
		// no-op
		return null;
	}

	protected String concatenate(COLUMN_TYPE primaryColumnValue, SECONDARY_COLUMN_TYPE secondaryColumnValue)
	{
		return new StringBuilder().append(primaryColumnValue).append(secondaryColumnValue).toString();
	}
	
}
