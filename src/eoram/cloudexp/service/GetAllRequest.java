package eoram.cloudexp.service;

/**
 * Represents a get request.
 *
 */
public class GetAllRequest extends Request 
{
	public GetAllRequest(String k) { super(k); }

	// default visibility
	GetAllRequest(long rid, String k) { super(rid, k); }

	@Override
	public RequestType getType() { return RequestType.GETALL; }
	
	
	@Override
	public String toString() 
	{
		String ret = super.toString();
		return ret;
	}
}
