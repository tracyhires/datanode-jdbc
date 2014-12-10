package com.dendreon.datatree.propertymapping;

import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class LocalDateTimePlusTZPropertyMapping extends ConcatenatedPropertyMapping<LocalDateTime, String> {

	private final DateTimeFormatter fmt;

	public LocalDateTimePlusTZPropertyMapping(String propertyId, String primaryColumnId, String secondaryColumnId)
	{
		this(propertyId, primaryColumnId, secondaryColumnId, DateTimeFormat.forPattern("h:mm a"));
	}

	public LocalDateTimePlusTZPropertyMapping(String propertyId, String primaryColumnId, String secondaryColumnId, DateTimeFormatter fmt)
	{
		super(propertyId, primaryColumnId, secondaryColumnId);
		this.fmt = fmt;
	}

	@Override
	public LocalDateTime toColumnType(String propertyValue) {
		return new LocalDateTime(propertyValue);
	}

	@Override
	protected String concatenate(LocalDateTime dateTime, String tzId)
	{
		if (dateTime != null)
		{
			String vTimeString = fmt.print(dateTime);
			if (tzId != null)
			{
				vTimeString += tzId;
			}
			return vTimeString;
		}
		return null;
	}
	
}
