package com.dendreon.datatree.propertymapping;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.slf4j.LoggerFactory;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;

public class LocalDateTimeToDateTimePropertyMapping implements PropertyMapping<DateTime, LocalDateTime> {

	private final String propertyId;
	private final String columnId;
	private final String timeZoneColumnId;
	private final DateTimeZone timeZone;
	
	//a list of 3 and 4 letter abbreviations for time zones we expect to see
	private static final Map<String, String> TIMEZONE_ABBREVIATIONS = new HashMap<String, String>() {{
		put("AKST", "America/Anchorage");
		put("AKDT", "America/Anchorage");
		put("UTC", "UTC");
		put("GMT", "Europe/London");
		put("BST", "Europe/London");
		put("CET", "Europe/Madrid");
		put("CEST", "Europe/Madrid");
		put("CDT", "America/Chicago");
		put("CST", "America/Chicago");
		put("EDT", "America/New_York");
		put("EST", "America/New_York");
		put("EEST", "Europe/Athens");
		put("EET", "Europe/Athens");
		put("HST", "Pacific/Honolulu");
		put("HAST", "Pacific/Honolulu");
		put("HADT", "Pacific/Honolulu");
		put("MDT", "America/Denver");
		put("MST", "America/Denver");
		put("PDT", "America/Los_Angeles");
		put("PST", "America/Los_Angeles");
		put("WEST", "Europe/Lisbon");
		put("WET", "Europe/Lisbon");
	}};

	public LocalDateTimeToDateTimePropertyMapping(String propertyId, String columnId, String timeZoneColumnId)
	{
		this.propertyId = propertyId;
		this.columnId = columnId;
		this.timeZoneColumnId = timeZoneColumnId;
		timeZone = DateTimeZone.getDefault();
	}

	public LocalDateTimeToDateTimePropertyMapping(String propertyId, String columnId, DateTimeZone timeZone)
	{
		this.propertyId = propertyId;
		this.columnId = columnId;
		this.timeZoneColumnId = null;
		this.timeZone = timeZone;
	}

	@Override
	public LocalDateTime toColumnType(DateTime propertyValue) {
		return propertyValue.toLocalDateTime();
	}

	@Override
	public String getPropertyId() {
		return propertyId;
	}

	@Override
	public Class getPropertyType() {
		return DateTime.class;
	}

	@Override
	public DateTime getPropertyValue(InputSourceRecord record) {
		LocalDateTime localDateTime = (LocalDateTime)record.getValue(columnId);
		if (localDateTime == null)
		{
			return null;
		}
		DateTimeZone timeZoneForProperty = this.timeZone;
		if (timeZoneColumnId != null && record.getValue(timeZoneColumnId) != null)
		{
			//DateTimeZone does not support 3 & 4 letter abbreviations for time zones.  So if we get an abbr,
			//look it up in our map of expected abbreviations.
			String timeZoneId = ((String)record.getValue(timeZoneColumnId)).trim();
			if (timeZoneId.length() <= 4 && TIMEZONE_ABBREVIATIONS.containsKey(timeZoneId))
			{
				timeZoneId = TIMEZONE_ABBREVIATIONS.get(timeZoneId);
			}
			
			try
			{
				timeZoneForProperty = DateTimeZone.forID(timeZoneId);
			} catch(Exception ex)
			{
				LoggerFactory.getLogger(getClass()).warn("Unable to interpret timezone: [" + timeZoneId + "]. Using timezone " + timeZone.toString());
			}
		}
		
		return localDateTime.toDateTime(timeZoneForProperty);
	}

	@Override
	public String getColumnName() {
		return columnId;
	}
	
}
