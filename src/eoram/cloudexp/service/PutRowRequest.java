package eoram.cloudexp.service;

import eoram.cloudexp.data.*;

/**
 * Represents a put request.
 */
public class PutRowRequest extends Request 
{
	protected DataItem val = null;
	
	public String leftID, rightID, leftLeaf, rightLeaf = ""; 
	
	public PutRowRequest(String k, DataItem v) { super(k); val = v; }
	
	PutRowRequest(long rid, String k, DataItem v) { super(rid, k); val = v; }
	
	PutRowRequest(long rid, String k, DataItem v, String li, String ri) { super(rid, k); val = v; leftID = li; rightID = ri; }

	@Override
	public RequestType getType() { return RequestType.PUTROW; }

	public DataItem getValue() { return val; }
	
	public void setValue(DataItem di) { val = di; }

	@Override
	public String toString() 
	{
		String ret = super.toString();
		ret += ", " + getValue().toString();
		
		return ret;
	}	
}
