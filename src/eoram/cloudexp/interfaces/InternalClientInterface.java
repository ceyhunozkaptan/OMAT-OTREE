package eoram.cloudexp.interfaces;

import java.io.File;
import eoram.cloudexp.service.*;

/**
 * Defines the internal interface of a client (i.e., an ORAM scheme).
 */
public interface InternalClientInterface 
{
	public void open(ExternalStorageInterface storage, File stateFile, boolean reset);
	
	public boolean isSynchronous();
	public String getName();
	
	public ScheduledRequest scheduleGet(GetRequest req);
	public ScheduledRequest scheduleGetAll(GetAllRequest req);
	public ScheduledRequest scheduleGetRow(GetRowRequest req);
	public ScheduledRequest scheduleGetCol(GetColRequest req);
	
	public ScheduledRequest schedulePut(PutRequest req);
	public ScheduledRequest schedulePutRow(PutRowRequest req);
	public ScheduledRequest schedulePutCol(PutColRequest req);

	public void close(String cloneStorageTo);

	public long peakByteSize();
}
