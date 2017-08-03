package eoram.cloudexp.service;

/**
 * Represents a get request.
 *
 */
public class GetColRequest extends Request 
{
	public GetColRequest(String k) { super(k); }

	// default visibility
	GetColRequest(long rid, String k) { super(rid, k); }

	@Override
	public RequestType getType() { return RequestType.GETCOL; }
	
	
	@Override
	public String toString() 
	{
		String ret = super.toString();
		return ret;
	}
}
