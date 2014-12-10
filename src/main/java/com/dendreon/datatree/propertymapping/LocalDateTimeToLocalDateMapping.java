package com.dendreon.datatree.propertymapping;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;

/**
 * Created with IntelliJ IDEA.
 * User: developer
 * Date: 8/27/13
 * Time: 6:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class LocalDateTimeToLocalDateMapping implements PropertyMapping<LocalDate, LocalDateTime>
{
    private final String propertyId;
    private final String columnName;
    private LocalTime localTime;

    public LocalDateTimeToLocalDateMapping(String propertyId, String columnName)
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
        return ((LocalDateTime) record.getValue(columnName)).toLocalDate();
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public LocalDateTime toColumnType(LocalDate propertyValue)
    {
        throw new UnsupportedOperationException("Cannot convert from LocalDate to LocalDateTime.  Loss of precision");
    }
}
