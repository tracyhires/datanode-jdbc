package com.dendreon.datatree.propertymapping;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;

import org.joda.time.DateTime;
import org.joda.time.LocalTime;

/**
 * Created with IntelliJ IDEA.
 * User: developer
 * Date: 8/27/13
 * Time: 6:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class DateTimeToLocalTimeMapping implements PropertyMapping<LocalTime, DateTime>
{
    private final String propertyId;
    private final String columnName;

    public DateTimeToLocalTimeMapping(String propertyId, String columnName)
    {
        this.propertyId = propertyId;
        this.columnName = columnName;
    }
    @Override
    public String getPropertyId()
    {
        return propertyId;
    }

    @Override
    public Class getPropertyType()
    {
        return LocalTime.class;
    }

    @Override
    public LocalTime getPropertyValue(InputSourceRecord record)
    {
        return ((DateTime) record.getValue(columnName)).toLocalTime();
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public DateTime toColumnType(LocalTime propertyValue)
    {
        throw new UnsupportedOperationException("Cannot convert from LocalTime to DateTime.  Loss of precision");
    }
}
