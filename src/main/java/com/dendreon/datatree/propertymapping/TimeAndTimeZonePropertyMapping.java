package com.dendreon.datatree.propertymapping;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;

public abstract class TimeAndTimeZonePropertyMapping<PROPERTY_TYPE, COLUMN_TYPE, TEST_COLUMN_TYPE> implements PropertyMapping<PROPERTY_TYPE, COLUMN_TYPE> {
	
	String propertyId;
	String testColumnId;
	TEST_COLUMN_TYPE testColumnValue;
	String primaryColumnId;
	String secondaryColumnId;

	public TimeAndTimeZonePropertyMapping(String propertyId, String testColumnId, TEST_COLUMN_TYPE testColumnValue, String primaryColumnId, String secondaryColumnId)
	{
		this.propertyId = propertyId;
		this.testColumnId = testColumnId;
		this.testColumnValue = testColumnValue;
		this.primaryColumnId = primaryColumnId;
		this.secondaryColumnId = secondaryColumnId;
	}
	
	@Override
	public String getPropertyId() {
		return propertyId;
	}
	
	@Override
	public String getColumnName() {
		return primaryColumnId;
	}
	
	@Override
	public PROPERTY_TYPE getPropertyValue(InputSourceRecord record)
	{
		return toPropertyType(getColumnValue(record));
	}

	protected COLUMN_TYPE getColumnValue(InputSourceRecord record) {
		TEST_COLUMN_TYPE condition = (TEST_COLUMN_TYPE)record.getValue(testColumnId);
		//if the condition matches return the value in the secondary column, else return the value in the primary column
		if (condition != null && condition.equals(testColumnValue))
		{
			return (COLUMN_TYPE) record.getValue(secondaryColumnId);
		}
		return (COLUMN_TYPE) record.getValue(primaryColumnId);
	}

	public abstract PROPERTY_TYPE toPropertyType(COLUMN_TYPE aValue);
}
