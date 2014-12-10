package com.dendreon.datatree.propertymapping;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.PropertyMapping;

public class OrdinalToEnumPropertyMapping<T extends Enum<T> > implements PropertyMapping<T, Integer> {

	private final String propertyId;
	private final String columnName;
	private final Class<T> enumClass;
	private final T[] values;
	private final int ordinalOffset;
	
	public OrdinalToEnumPropertyMapping(String propertyId, String columnName, Class<T> enumClass) {
		this(propertyId, columnName, enumClass, 0);
	}
	
	public OrdinalToEnumPropertyMapping(String propertyId, String columnName, Class<T> enumClass, int ordinalOffset) {
		this.propertyId = propertyId;
		this.columnName = columnName;
		this.enumClass = enumClass;
		this.ordinalOffset = ordinalOffset;
		values = enumClass.getEnumConstants();
	}

	@Override
	public String getPropertyId() {
		return propertyId;
	}

	@Override
	public Class getPropertyType() {
		return enumClass;
	}

	@Override
	public T getPropertyValue(InputSourceRecord record) {
		if(record.getValue(columnName) == null) {
			return null;
		}
		int enumOrdinal = ((Integer)record.getValue(columnName) - ordinalOffset);
		if (enumOrdinal > 0 && enumOrdinal < values.length) {
			return values[enumOrdinal];
		}
		return null;
	}

	@Override
	public String getColumnName() {
		return columnName;
	}

	@Override
	public Integer toColumnType(T propertyValue) {
		if (propertyValue == null) {
			return null;
		}
		return propertyValue.ordinal() + ordinalOffset;
	}

}
