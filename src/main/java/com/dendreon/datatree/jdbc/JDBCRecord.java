package com.dendreon.datatree.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.dendreon.datatree.InputSourceRecord;
import com.dendreon.datatree.OutputSourceRecord;

public class JDBCRecord implements InputSourceRecord, OutputSourceRecord
{
    private final ResultSet resultSet;
    private final ResultSetMetaData metaData;
    private final Map<String, Object> data;

    public JDBCRecord(ResultSet resultSet)
    {
        this.resultSet = resultSet;
        data = new HashMap<String, Object>();
        if (resultSet != null) {
        	try {
				metaData = resultSet.getMetaData();
	        	if (!resultSet.isAfterLast() && !resultSet.isBeforeFirst()) {
	        		for(int i = 1; i <= metaData.getColumnCount(); i++) {
	        			data.put(metaData.getColumnName(i), resultSet.getObject(i));
	        		}
	        	}
			} catch (SQLException e) {
				throw new RuntimeException("Unable to initialize record from resultset.", e);
			}
        } else {
        	metaData = null;
        }
    }

    public Object getValue(String property)
    {
        if (property != null && data.containsKey(property))
        {
        	return data.get(property);
        }

        return null;
    }

	@Override
	public void setValue(String column, Object value) {
		data.put(column, value);
		//TODO: update the record
	}
	
	@Override
	public boolean equals(Object other) {
		if (other == null) {
			return false;
		}
		if (other instanceof JDBCRecord) {
			JDBCRecord otherRecord = (JDBCRecord)other;
			if (this == otherRecord) {
				return true;
			}
			if (otherRecord.metaData != metaData) {
				if (metaData != null && otherRecord.metaData != null) {
					if (!metaData.equals(otherRecord.metaData)) {
						return false;
					}
				} else {
					return false;
				}
			}
			if (data.size() != otherRecord.data.size()) {
				return false;
			}
			for(Map.Entry<String, Object> entry : data.entrySet()) {
				Object thisPropertyValue = entry.getValue();
				Object otherPropertyValue = otherRecord.data.get(entry.getKey());
				if(thisPropertyValue != otherPropertyValue) {
					if (thisPropertyValue != null && otherPropertyValue != null) {
						if (!thisPropertyValue.equals(otherPropertyValue)) {
							return false;
						}
					} else {
						return false;
					}
				}
			}
		}
		return true;
	}
	
	@Override
	public int hashCode() {
		int hashCode = 0;
		if (this.metaData != null) {
			hashCode ^= metaData.hashCode();
		}
		for(Map.Entry<String, Object> entry : data.entrySet()) {
			Object propertyValue = entry.getValue();
			if (propertyValue != null) {
				hashCode ^= propertyValue.hashCode();
			}
		}
		return hashCode;
	}
}
