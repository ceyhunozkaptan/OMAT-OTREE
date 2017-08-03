package eoram.cloudexp.service;

import java.util.ArrayList;

/** 
 * Represents a download storage operation.
 */
public class DownloadBulkOperation extends Operation 
{
	ArrayList<byte[]> dataList = new ArrayList<byte[]>();
	int dim = -1;
	
	public DownloadBulkOperation(long r, String k, int dim) { 
		super(r, k); 
		dim = this.dim;
	}
	
	protected DownloadBulkOperation(long r, long o, String k, int dim) // constructor (unsafe)
	{
		super(r, k);
		opId = o;
		dim = this.dim;
	}

	public void setData(ArrayList<byte[]> d) { dataList = d; }
	
	public int getDimension () { return dim; }
	
	@Override
	public OperationType getType() { return OperationType.DOWNLOADBULK; }
}
