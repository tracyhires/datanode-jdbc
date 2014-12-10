package com.dendreon.datatree.propertymapping;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;

/**
 * Created with IntelliJ IDEA.
 * User: developer
 * Date: 8/27/13
 * Time: 6:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class LocalDateTimeToLocalTimeMapping implements PropertyMapping<LocalTime, LocalDateTime>
{
    private final String propertyId;
    private final String columnName;
    private LocalTime localTime;

    public LocalDateTimeToLocalTimeMapping(String propertyId, String columnName)
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
        return ((LocalDateTime) record.getValue(columnName)).toLocalTime();
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public LocalDateTime toColumnType(LocalTime propertyValue)
    {
        throw new UnsupportedOperationException("Cannot convert from LocalTime to LocalDateTime.  Loss of precision");
    }
}
