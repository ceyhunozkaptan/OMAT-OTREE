package eoram.cloudexp.interfaces;

import java.util.ArrayList;

import eoram.cloudexp.service.*;

/**
 * Defines the internal interface of a storage system.
 */
public interface InternalStorageInterface 
{
	public void connect();
	
	public ScheduledOperation downloadObject(DownloadOperation op);
	public ArrayList<ScheduledOperation> downloadPath(ArrayList<DownloadOperation> op);
	public ScheduledOperation downloadRow(DownloadBulkOperation op);
	public ScheduledOperation downloadAll(DownloadBulkOperation op);
	public ArrayList<ScheduledOperation> downloadRow(ArrayList<DownloadOperation> op);
	public ScheduledOperation downloadCol(DownloadBulkOperation op);
	public ArrayList<ScheduledOperation> downloadCol(ArrayList<DownloadOperation> op);
	
	public ScheduledOperation uploadObject(UploadOperation op);
	public ArrayList<ScheduledOperation> uploadPath(ArrayList<UploadOperation> op);
	public ArrayList<ScheduledOperation> uploadAll(ArrayList<UploadOperation> op);
	public ArrayList<ScheduledOperation> uploadRow(ArrayList<UploadOperation> op);
	public ArrayList<ScheduledOperation> uploadAllRow(ArrayList<UploadOperation> op);
	
	public ArrayList<ScheduledOperation> uploadRowPath(ArrayList<UploadOperation> op);
	public ArrayList<ScheduledOperation> uploadColPath(ArrayList<UploadOperation> op);
	
	public ScheduledOperation deleteObject(DeleteOperation op);
	public ScheduledOperation copyObject(CopyOperation op);
	
	public ScheduledOperation listObjects(ListOperation op);
	
	public void disconnect();
	
	public long totalByteSize();
	
	public void cloneTo(String to);


}
