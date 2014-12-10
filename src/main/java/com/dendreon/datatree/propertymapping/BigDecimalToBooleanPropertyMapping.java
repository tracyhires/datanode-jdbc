package com.dendreon.datatree.propertymapping;

import java.math.BigDecimal;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;

/**
 * Created with IntelliJ IDEA.
 * User: developer
 * Date: 8/27/13
 * Time: 6:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class BigDecimalToBooleanPropertyMapping implements PropertyMapping<Boolean, BigDecimal>
{
    private final String propertyId;
    private final String columnName;
    private final BigDecimal trueValue;
    private final BigDecimal falseValue;

    public BigDecimalToBooleanPropertyMapping(String propertyId, String columnName, BigDecimal trueValue)
    {
        this.propertyId = propertyId;
        this.columnName = columnName;
        this.trueValue = trueValue;
        this.falseValue = trueValue.equals(new BigDecimal(0)) ? new BigDecimal(1) : new BigDecimal(0);
    }

    public BigDecimalToBooleanPropertyMapping(String propertyId, String columnName)
    {
        this.propertyId = propertyId;
        this.columnName = columnName;
        this.trueValue = new BigDecimal(1);
        this.falseValue = new BigDecimal(0);
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
    	BigDecimal recordValue = (BigDecimal)record.getValue(columnName);
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
    public BigDecimal toColumnType(Boolean propertyValue)
    {
    	if (propertyValue != null)
    	{
    		return (propertyValue == true) ? trueValue :  falseValue;
    	}
    	return null;
    }
}
