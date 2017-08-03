package eoram.cloudexp.service;

/** 
 * Represents a download storage operation.
 */
public class DownloadOperation extends Operation 
{
	protected byte[] data = null;
	protected int dim = -1;
	
	
	public DownloadOperation(long r, String k) { super(r, k); }
	
	public DownloadOperation(long r, String k, int dim) { 
		super(r, k); 
		dim = this.dim;
	}
	
	protected DownloadOperation(long r, long o, String k) // constructor (unsafe)
	{
		super(r, k);
		opId = o;
	}

	public void setData(byte[] d) { data = d; }
	
	@Override
	public OperationType getType() { return OperationType.DOWNLOAD; }
	
	public int getDimension () { return dim; }
}
