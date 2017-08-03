package eoram.cloudexp.implementation;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SystemParameters;
import eoram.cloudexp.data.EmptyDataItem;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.data.WrappedListDataItem;
import eoram.cloudexp.interfaces.*;
import eoram.cloudexp.service.CopyOperation;
import eoram.cloudexp.service.DeleteOperation;
import eoram.cloudexp.service.DownloadBulkOperation;
import eoram.cloudexp.service.DownloadOperation;
import eoram.cloudexp.service.ListOperation;
import eoram.cloudexp.service.ScheduledOperation;
import eoram.cloudexp.service.UploadOperation;
import eoram.cloudexp.utils.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements a (synchronous) storage interface for the local filesystem.
 * <p><p>
 * The primary use of storage interfaces for the local filesystem is testing/debugging. 
 * In other words, this class can be used to test new (ORAM) clients locally before running them on S3.
 * <p>
 * @see AsyncLocalStorage
 */
public class LocalStorage implements InternalStorageInterface 
{
	private Log log = Log.getInstance();
	
	private SystemParameters sysParams = SystemParameters.getInstance();
	private String directoryFP = null;
	
	private boolean reset = false;
	
	public LocalStorage() { this(true); }
	
	public LocalStorage(String dirFP, boolean rst) { directoryFP = dirFP; reset = rst; }
	
	public LocalStorage(boolean rst) { this(null, rst); }
	
	private void initialize(String dirFP) 
	{
		directoryFP = dirFP;
		
		FileUtils.getInstance().initializeDirectory(directoryFP, reset);
	}

	@Override
	public void connect() 
	{
		initialize((directoryFP == null) ? sysParams.localDirectoryFP : directoryFP);
	}

	@Override
	public ScheduledOperation downloadObject(DownloadOperation op) 
	{
		String key = op.getKey();
		
		ScheduledOperation sop = new ScheduledOperation(op);
		{
			String fp = directoryFP + "/" + key;
			try 
			{
				File f = new File(fp); byte[] d = new byte[(int) f.length()];
				//System.out.println("[LS (downloadObject)] downloading key: " + key);
				// -d-
				//{ log.append("[LS (downloadObject)] downloading key: " + key, Log.TRACE); } // debug only
				
				FileInputStream fi = new FileInputStream(fp);
				fi.read(d);
				fi.close();
				
				sop.onSuccess(new SimpleDataItem(d));
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
				//throw new RuntimeException(e.getMessage());
				sop.onFailure();
			}
		}
		
		return sop;
	}
	
	// FIXME
	@Override
	public ScheduledOperation downloadRow(DownloadBulkOperation op) 
	{
		String key = op.getKey();
		
		ScheduledOperation sop = new ScheduledOperation(op);
		{
			String fp = directoryFP + "/" + key;
			try 
			{
				File f = new File(fp); byte[] d = new byte[(int) f.length()];
				//System.out.println("[LS (downloadObject)] downloading key: " + key);
				// -d-
				//{ log.append("[LS (downloadObject)] downloading key: " + key, Log.TRACE); } // debug only
				
				FileInputStream fi = new FileInputStream(fp);
				fi.read(d);
				fi.close();
				
				sop.onSuccess(new SimpleDataItem(d));
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
				//throw new RuntimeException(e.getMessage());
				sop.onFailure();
			}
		}
		
		return sop;
	}
	
	// FIXME
	@Override
	public ScheduledOperation downloadCol(DownloadBulkOperation op) 
	{
		String key = op.getKey();
		
		ScheduledOperation sop = new ScheduledOperation(op);
		{
			String fp = directoryFP + "/" + key;
			try 
			{
				File f = new File(fp); byte[] d = new byte[(int) f.length()];
				//System.out.println("[LS (downloadObject)] downloading key: " + key);
				// -d-
				//{ log.append("[LS (downloadObject)] downloading key: " + key, Log.TRACE); } // debug only
				
				FileInputStream fi = new FileInputStream(fp);
				fi.read(d);
				fi.close();
				
				sop.onSuccess(new SimpleDataItem(d));
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
				//throw new RuntimeException(e.getMessage());
				sop.onFailure();
			}
		}
		
		return sop;
	}
	
	@Override
	public ScheduledOperation uploadObject(UploadOperation op) 
	{
		String key = op.getKey();
		byte[] data = op.getDataItem().getData();
		
		ScheduledOperation sop = new ScheduledOperation(op);
		{
			String fp = directoryFP + "/" + key;
			try 
			{
				// -d-
				//{ log.append("[LS (uploadObject)] uploading key: " + key, Log.TRACE); } // debug only
				//System.out.println("[LS (uploadObject)] uploading key: " + key);
				FileOutputStream f = new FileOutputStream(fp);
				f.write(data);
				f.close();
				
				sop.onSuccess(new EmptyDataItem());
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
				//throw new RuntimeException(e.getMessage());
				sop.onFailure();
			}
		}
		return sop;
	}
	
	// FIXME
	@Override
	public ArrayList<ScheduledOperation> uploadRow(ArrayList<UploadOperation> op) 
	{
		return new ArrayList<ScheduledOperation>();

	}
	
	// FIXME
	@Override
	public ArrayList<ScheduledOperation> uploadRowPath(ArrayList<UploadOperation> op) 
	{
		return new ArrayList<ScheduledOperation>();

	}
	
	// FIXME
	@Override
	public ArrayList<ScheduledOperation> uploadColPath(ArrayList<UploadOperation> op) 
	{
		return new ArrayList<ScheduledOperation>();
	
	}
	
	@Override
	public ScheduledOperation deleteObject(DeleteOperation op) 
	{
		String key = op.getKey();
		
		ScheduledOperation sop = new ScheduledOperation(op);
		{
			//{ log.append("[LS (deleteObject)] deleting key: " + key, Log.TRACE); } // debug only
			
			String fp = directoryFP + "/" + key;
			File f = new File(fp); 
			
			if(f.delete() == true) { sop.onSuccess(new EmptyDataItem()); }
			else { sop.onFailure(); }
		}
		return sop;
	}
	
	@Override
	public ScheduledOperation copyObject(CopyOperation op) 
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		{
			String srcFP = directoryFP + "/" + op.getSourceKey(); File srcFile = new File(srcFP); 
			String destFP = directoryFP + "/" + op.getDestinationKey(); File destFile = new File(destFP); 
			
			boolean success = FileUtils.getInstance().copy(srcFile, destFile);
			if(success == true) { sop.onSuccess(new EmptyDataItem()); }
			else { sop.onFailure(); }
		}
		return sop;
	}
	
	@Override
	public ScheduledOperation listObjects(ListOperation op) 
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		
		List<String> ret = FileUtils.getInstance().listFilenames(directoryFP + "/");
		sop.onSuccess(new WrappedListDataItem(ret));
		
		return sop;
	}

	@Override
	public void disconnect() { ; }

	@Override
	public long totalByteSize() 
	{
		return FileUtils.getInstance().flatDirectoryByteSize(directoryFP);
	}
	
	@Override
	public void cloneTo(String to) 
	{
		FileUtils.getInstance().flatDirectoryCopy(directoryFP, to);
	}

	@Override
	public ArrayList<ScheduledOperation> downloadPath(ArrayList<DownloadOperation> op) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArrayList<ScheduledOperation> uploadPath(ArrayList<UploadOperation> op) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArrayList<ScheduledOperation> downloadRow(ArrayList<DownloadOperation> op) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArrayList<ScheduledOperation> downloadCol(ArrayList<DownloadOperation> op) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ScheduledOperation downloadAll(DownloadBulkOperation op) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArrayList<ScheduledOperation> uploadAll(ArrayList<UploadOperation> op) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArrayList<ScheduledOperation> uploadAllRow(ArrayList<UploadOperation> op) {
		// TODO Auto-generated method stub
		return null;
	}
}
