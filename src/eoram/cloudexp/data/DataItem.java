package eoram.cloudexp.data;

import javax.xml.bind.DatatypeConverter;

/**
 * Represents an abstract data item.
 */
public abstract class DataItem 
{
	public abstract byte[] getData();
	
	@Override
	public synchronized String toString()
	{
		return DatatypeConverter.printBase64Binary(getData());
	}
}
