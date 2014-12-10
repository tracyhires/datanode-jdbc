package com.dendreon.datatree.propertymapping;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;

/**
 * Created with IntelliJ IDEA.
 * User: developer
 * Date: 8/27/13
 * Time: 6:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class DateTimeToLocalDateMapping implements PropertyMapping<LocalDate, DateTime>
{
    private final String propertyId;
    private final String columnName;

    public DateTimeToLocalDateMapping(String propertyId, String columnName)
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
        return LocalDate.class;
    }

    @Override
    public LocalDate getPropertyValue(InputSourceRecord record)
    {
        return ((DateTime) record.getValue(columnName)).toLocalDate();
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public DateTime toColumnType(LocalDate propertyValue)
    {
        throw new UnsupportedOperationException("Cannot convert from LocalDate to DateTime.  Loss of precision");
    }
}
