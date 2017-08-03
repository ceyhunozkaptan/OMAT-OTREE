package eoram.cloudexp.service;

/**
 * Represents a get request.
 *
 */
public class GetRequest extends Request 
{
	public GetRequest(String k) { super(k); }

	// default visibility
	GetRequest(long rid, String k) { super(rid, k); }

	@Override
	public RequestType getType() { return RequestType.GET; }
	
	
	@Override
	public String toString() 
	{
		String ret = super.toString();
		return ret;
	}
}
