package com.dendreon.datatree.propertymapping;

import org.joda.time.LocalTime;

import com.dendreon.datatree.DataNode;
import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;

public class LocalTimePlusMinutesOffsetPropertyMapping implements
		PropertyMapping<LocalTime, Integer> {

	private static final int MINUTES_IN_DAY = 24 * 60;
	private final String propertyId;
	private final DataNode localTimeDataNode;
	private final String columnName;
	private final String localTimePropertyId;
	
	public LocalTimePlusMinutesOffsetPropertyMapping(String propertyId, String columnName, DataNode localTimeDataNode, String localTimePropertyId) {
		this.propertyId = propertyId;
		this.columnName = columnName;
		this.localTimeDataNode = localTimeDataNode;
		this.localTimePropertyId = localTimePropertyId;
	}
	
	@Override
	public String getPropertyId() {
		return propertyId;
	}

	@Override
	public Class getPropertyType() {
		return LocalTime.class;
	}

	@Override
	public LocalTime getPropertyValue(InputSourceRecord record) {
		LocalTime baseTime = (LocalTime)localTimeDataNode.getPropertyValue(localTimePropertyId);
		Integer plusMinutes = (Integer)record.getValue(columnName);
		if (plusMinutes == null) {
			return null;
		}
		else if (baseTime == null) {
			baseTime = LocalTime.MIDNIGHT;
		}
		return baseTime.plusMinutes(plusMinutes);
	}

	@Override
	public String getColumnName() {
		return columnName;
	}

	@Override
	public Integer toColumnType(LocalTime propertyValue) {
		if (propertyValue == null) {
			return null;
		}
		LocalTime baseTime = (LocalTime)localTimeDataNode.getPropertyValue(localTimePropertyId);
		if (baseTime == null) {
			baseTime = LocalTime.MIDNIGHT;
		}
		
		int minutesAfterMidnight = propertyValue.getHourOfDay() * 60 + propertyValue.getMinuteOfHour();
		int baseMinutesAfterMidnight = baseTime.getHourOfDay() * 60 + baseTime.getMinuteOfHour();
		
		return (minutesAfterMidnight - baseMinutesAfterMidnight) % MINUTES_IN_DAY;
	}

}
