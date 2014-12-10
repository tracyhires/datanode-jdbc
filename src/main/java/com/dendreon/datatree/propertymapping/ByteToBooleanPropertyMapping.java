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
public class ByteToBooleanPropertyMapping implements PropertyMapping<Boolean, Byte>
{
    private final String propertyId;
    private final String columnName;
    private final Byte trueValue;
    private final Byte falseValue;

    public ByteToBooleanPropertyMapping(String propertyId, String columnName, Byte trueValue)
    {
        this.propertyId = propertyId;
        this.columnName = columnName;
        this.trueValue = trueValue;
        this.falseValue = trueValue.equals((byte)0) ? (byte) 1 : (byte)0;
    }

    public ByteToBooleanPropertyMapping(String propertyId, String columnName)
    {
        this.propertyId = propertyId;
        this.columnName = columnName;
        this.trueValue =(byte)1;
        this.falseValue = (byte)0;
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
    	Byte recordValue = (Byte)record.getValue(columnName);
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
    public Byte toColumnType(Boolean propertyValue)
    {
    	if (propertyValue != null)
    	{
    		return (propertyValue == true) ? trueValue :  falseValue;
    	}
    	return null;
    }
}
