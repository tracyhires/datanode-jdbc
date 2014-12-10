package com.dendreon.datatree.propertymapping;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;

/**
 * Created with IntelliJ IDEA.
 * User: developer
 * Date: 8/27/13
 * Time: 6:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class LongToBooleanPropertyMapping implements PropertyMapping<Boolean, Long>
{
    private final String propertyId;
    private final String columnName;
    private final Long trueValue;
    private final Long falseValue;
    
    public LongToBooleanPropertyMapping(String propertyId, String columnName)
    {
        this.propertyId = propertyId;
        this.columnName = columnName;
        this.trueValue = 1L;
        this.falseValue = 0L;
    }

    public LongToBooleanPropertyMapping(String propertyId, String columnName, Long trueValue)
    {
        this.propertyId = propertyId;
        this.columnName = columnName;
        this.trueValue = trueValue;
        this.falseValue = (trueValue.equals(0L)) ? 1L : 0L;
    }

    @Override
    public String getPropertyId()
    {
        return propertyId;
    }

    @Override
    public Class getPropertyType()
    {
        return Boolean.class;
    }

    @Override
    public Boolean getPropertyValue(InputSourceRecord record)
    {
    	Long recordValue = (Long)record.getValue(columnName);
    	if (recordValue != null)
    	{
    		return recordValue.equals(trueValue);
    	}
    	
    	return null;
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public Long toColumnType(Boolean propertyValue)
    {
    	if (propertyValue != null)
    	{
    		return (propertyValue == true) ? trueValue : falseValue;
    	}
    	return null;
    }
}
