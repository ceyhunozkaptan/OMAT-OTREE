package eoram.cloudexp.service;

/**
 * Represents a get request.
 *
 */
public class GetRowRequest extends Request 
{
	public GetRowRequest(String k) { super(k); }

	// default visibility
	GetRowRequest(long rid, String k) { super(rid, k); }

	@Override
	public RequestType getType() { return RequestType.GETROW; }
	
	
	@Override
	public String toString() 
	{
		String ret = super.toString();
		return ret;
	}
}
