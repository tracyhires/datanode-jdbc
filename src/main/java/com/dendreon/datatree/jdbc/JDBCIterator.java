package com.dendreon.datatree.jdbc;

import com.dendreon.datatree.InputSourceIterator;
import com.dendreon.datatree.InputSourceRecord;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;

public class JDBCIterator implements InputSourceIterator
{
	private final ResultSet resultSet;

    public JDBCIterator(ResultSet resultSet)
    {
    	this.resultSet = resultSet;
    }

    public void remove()
    {
        throw new RuntimeException("Not supported.");
    }

    public boolean hasNext()
    {
    	try {
    		if (resultSet != null) {
				return !resultSet.isLast();
    		}
		} catch (SQLException e) {
			throw new RuntimeException("Unable to determine iterator position on resultSet", e);
		}
    	return false;
    }

    public InputSourceRecord next()
    {
    	try {
	        if (resultSet.next())
	        {
	        	return new JDBCRecord(resultSet);
	        }
	        else
	        {
	            throw new RuntimeException("No more records in the OData data source.");
	        }
    	} catch (SQLException e) {
            throw new RuntimeException("Could not advance to next record in result set.", e);
    	}
    }

    public void close()
    {
        try
        {
        	resultSet.close();
        }
        catch (SQLException exc)
        {
            LoggerFactory.getLogger(JDBCSource.class).warn("Unable to close the result set.", exc);
        }
    }
}
