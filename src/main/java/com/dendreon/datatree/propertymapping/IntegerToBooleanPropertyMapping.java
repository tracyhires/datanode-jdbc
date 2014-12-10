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
public class IntegerToBooleanPropertyMapping implements PropertyMapping<Boolean, Integer>
{
    private final String propertyId;
    private final String columnName;
    private final Integer trueValue;
    private final Integer falseValue;
    
    public IntegerToBooleanPropertyMapping(String propertyId, String columnName)
    {
        this.propertyId = propertyId;
        this.columnName = columnName;
        this.trueValue = 1;
        this.falseValue = 0;
    }

    public IntegerToBooleanPropertyMapping(String propertyId, String columnName, Integer trueValue)
    {
        this.propertyId = propertyId;
        this.columnName = columnName;
        this.trueValue = trueValue;
        this.falseValue = (trueValue.equals(0)) ? 1 : 0;
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
    	Integer recordValue = (Integer)record.getValue(columnName);
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
    public Integer toColumnType(Boolean propertyValue)
    {
    	if (propertyValue != null)
    	{
    		return (propertyValue == true) ? trueValue : falseValue;
    	}
    	return null;
    }
}
