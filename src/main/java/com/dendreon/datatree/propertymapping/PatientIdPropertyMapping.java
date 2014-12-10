package com.dendreon.datatree.propertymapping;

import java.math.BigDecimal;

public class PatientIdPropertyMapping extends ConditionalPropertyMapping<String, String, BigDecimal> {

	public PatientIdPropertyMapping(String propertyId, String clinicalColulmnName, String ccidColumnName, String subjectIdColumnName) {
		super(propertyId, clinicalColulmnName, new BigDecimal(1), ccidColumnName, subjectIdColumnName);
	}

	@Override
	public Class getPropertyType() {
		return String.class;
	}

	@Override
	public String toColumnType(String propertyValue) {
		return propertyValue;
	}

	@Override
	public String toPropertyType(String aValue) {
		return aValue;
	}

}
