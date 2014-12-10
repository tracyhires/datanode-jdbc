package com.dendreon.datatree.propertymapping;

import java.util.Arrays;
import java.util.List;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;

public class ValueEqualsPropertyMapping<COLUMN_TYPE> implements
		PropertyMapping<Boolean, COLUMN_TYPE> {

	private final String propertyId;
	private final String columnId;
	private final List<COLUMN_TYPE> matchedValues;
	public ValueEqualsPropertyMapping(String propertyId, String columnId, COLUMN_TYPE...matchValues)
	{
		this.propertyId = propertyId;
		this.columnId = columnId;
		this.matchedValues = Arrays.asList(matchValues);
	}
	@Override
	public String getPropertyId() {
		return propertyId;
	}

	@Override
	public Class getPropertyType() {
		return Boolean.class;
	}

	@Override
	public Boolean getPropertyValue(InputSourceRecord record) {
		COLUMN_TYPE recordValue = (COLUMN_TYPE)record.getValue(columnId);
		if (recordValue == null || matchedValues == null)
		{
			return null;
		}
		return matchedValues.contains(recordValue);
	}

	@Override
	public String getColumnName() {
		return columnId;
	}

	@Override
	public COLUMN_TYPE toColumnType(Boolean propertyValue) {
		// no-op
		return null;
	}

}
