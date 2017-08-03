package eoram.cloudexp.implementation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import eoram.cloudexp.evaluation.PerformanceEvaluationLogger;
import eoram.cloudexp.interfaces.ExternalStorageInterface;
import eoram.cloudexp.interfaces.InternalStorageInterface;
import eoram.cloudexp.service.*;
import eoram.cloudexp.utils.Errors;

/**
 * Implements an adapter between the external storage interface and the internal storage's implementation.
 * <p><p>
 * The adapter takes care of handling performance events and completion callbacks so that the storages' implementations do not need to.
 */
public class StorageAdapter implements ExternalStorageInterface
{
	protected PerformanceEvaluationLogger pe = PerformanceEvaluationLogger.getInstance();

	protected InternalStorageInterface storage = null;
//	protected CompletionThread completionThread = null;
	
	protected boolean opened = false;
	
//	public class CompletionThread extends Thread
//	{
//		protected volatile boolean done = false;		
//		protected BlockingQueue<ScheduledOperation> queue = null;
//		
//		public CompletionThread(BlockingQueue<ScheduledOperation> q) { queue = q; }
//		
//		public void run()
//		{
//			Set<ScheduledOperation> pending = new HashSet<ScheduledOperation>();
//			
//			while(done == false || queue.size() > 0 || pending.size() > 0)
//			{
//				// drain stuff to pending
//				queue.drainTo(pending);
//				
//				if(pending.size() == 0) 
//				{ 
//					// poll 
////					try 
////					{
////						ScheduledOperation sop = queue.poll(0, TimeUnit.MILLISECONDS);
//						ScheduledOperation sop = queue.poll();
//						if(sop != null) { pending.add(sop); }
////					} 
////					catch (InterruptedException e1) {	e1.printStackTrace(); }
//					continue; 
//				}
//				Iterator<ScheduledOperation> iter = pending.iterator();
//				while(iter.hasNext() == true)
//				{
//					ScheduledOperation sop = iter.next();
//					if(sop.isReady() == true)
//					{						
//						iter.remove(); // remove
//						
//						complete(sop); // complete the operation
//					}
//				}
//			}
//		}
//		
//		public void shutdown()
//		{
//			done = true;
//		}
//	}
	
//	protected BlockingQueue<ScheduledOperation> scheduledQueue = new LinkedBlockingQueue<ScheduledOperation>();
	
	public StorageAdapter(InternalStorageInterface s)
	{
		storage = s; opened = false;
//		completionThread = new CompletionThread(scheduledQueue);
	}
	
	protected void complete(ScheduledOperation sop)
	{
		assert(sop.isReady() == true);
		
		pe.completeOperation(sop); // -----------------
	}
	
	@Override
	public void connect() 
	{
//		completionThread.start();
		
		pe.connectCall(); // -----------
		
		storage.connect();///////CREATES FILES AND DIRECTORY
		
		pe.connectDone(); // -----------
		
		opened = true;
	}

	@Override
	public ScheduledOperation downloadObject(DownloadOperation op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		pe.scheduleOperation(op); // -----------
		
		ScheduledOperation sop = storage.downloadObject(op);
		
		complete(sop);
		
//		try { scheduledQueue.put(sop); } catch (InterruptedException e) { e.printStackTrace(); }
		
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> downloadPath(ArrayList<DownloadOperation> op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		for (int i = 0; i < op.size(); i++){
			pe.scheduleOperation(op.get(i)); 
		}
		
		ArrayList<ScheduledOperation> sop = storage.downloadPath(op);
		
		for (int i = 0; i < op.size(); i++){
			complete(sop.get(i));
		}
		
//		try { 
//			for (int i = 0; i < op.size(); i++)
//				scheduledQueue.put(sop.get(i)); 
//		} 
//		catch (InterruptedException e) { 
//			e.printStackTrace(); 
//		}
		
		return sop;
	}

	@Override
	public ScheduledOperation downloadRow(DownloadBulkOperation op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		pe.scheduleOperation(op); // -----------
		
		ScheduledOperation sop = storage.downloadRow(op);
		
//		try { scheduledQueue.put(sop); } catch (InterruptedException e) { e.printStackTrace(); }
		
		complete(sop);
		
		return sop;
	}
	
	@Override
	public ScheduledOperation downloadAll(DownloadBulkOperation op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		pe.scheduleOperation(op); // -----------
		
		ScheduledOperation sop = storage.downloadAll(op);
		
		complete(sop);
		
//		try { scheduledQueue.put(sop); } catch (InterruptedException e) { e.printStackTrace(); }
		
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> downloadRow(ArrayList<DownloadOperation> op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		for (int i = 0; i < op.size(); i++){
			pe.scheduleOperation(op.get(i)); 
		}
		
		ArrayList<ScheduledOperation> sop = storage.downloadPath(op);
		
		for (int i = 0; i < op.size(); i++){
			complete(sop.get(i));
		}
		
//		try { 
//			for (int i = 0; i < op.size(); i++)
//				scheduledQueue.put(sop.get(i)); 
//		} 
//		catch (InterruptedException e) { 
//			e.printStackTrace(); 
//		}
		
		return sop;
	}
	
	//FIXME
	@Override
	public ScheduledOperation downloadCol(DownloadBulkOperation op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		pe.scheduleOperation(op); // -----------
		
		ScheduledOperation sop = storage.downloadCol(op);
		
		complete(sop);
		
//		try { scheduledQueue.put(sop); } catch (InterruptedException e) { e.printStackTrace(); }
		
		return sop;
	}
	
	@Override
	public ScheduledOperation uploadObject(UploadOperation op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		pe.scheduleOperation(op); // -----------
		
		ScheduledOperation sop = storage.uploadObject(op);
		
		complete(sop);
		
//		try { scheduledQueue.put(sop); } catch (InterruptedException e) { Errors.error(e); }
		
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadPath(ArrayList<UploadOperation> op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		for (int i = 0; i < op.size(); i++){
			pe.scheduleOperation(op.get(i)); 
		}
		
		ArrayList<ScheduledOperation> sop = storage.uploadPath(op);
		
		for (int i = 0; i < op.size(); i++){
			complete(sop.get(i));
		}
		
//		try { 
//			for (int i = 0; i < op.size(); i++)
//				scheduledQueue.put(sop.get(i)); 
//		} 
//		catch (InterruptedException e) { 
//			Errors.error(e); 
//		}
		
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadAll(ArrayList<UploadOperation> op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		for (int i = 0; i < op.size(); i++){
			pe.scheduleOperation(op.get(i)); 
		}
		
		ArrayList<ScheduledOperation> sop = storage.uploadAll(op);
		System.gc();
		for (int i = 0; i < op.size(); i++){
			complete(sop.get(i));
		}
		
//		try { 
//			for (int i = 0; i < op.size(); i++)
//				scheduledQueue.put(sop.get(i)); 
//		} 
//		catch (InterruptedException e) { 
//			Errors.error(e); 
//		}
		
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadRow(ArrayList<UploadOperation> op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		for (int i = 0; i < op.size(); i++){
			pe.scheduleOperation(op.get(i)); 
		}
		
//		ScheduledOperation sop = storage.uploadRow(op);
		
		ArrayList<ScheduledOperation> sop = storage.uploadRow(op);
		
		for (int i = 0; i < op.size(); i++){
			complete(sop.get(i));
		}
		
//		try { 
//			for (int i = 0; i < op.size(); i++)
//				scheduledQueue.put(sop.get(i)); 
//		} 
//		catch (InterruptedException e) { 
//			Errors.error(e); 
//		}
		
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadAllRow(ArrayList<UploadOperation> op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		for (int i = 0; i < op.size(); i++){
			pe.scheduleOperation(op.get(i)); 
		}
		
//		ScheduledOperation sop = storage.uploadRow(op);
		
		ArrayList<ScheduledOperation> sop = storage.uploadAllRow(op);
		System.gc();
		for (int i = 0; i < op.size(); i++){
			complete(sop.get(i));
		}
		
		op.clear();
		op = null;
		System.gc();
		
//		try { 
//			for (int i = 0; i < op.size(); i++){
//				scheduledQueue.put(sop.get(i)); 
//			}
				
//		} 
//		catch (InterruptedException e) { 
//			Errors.error(e); 
//		}
		
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadRowPath(ArrayList<UploadOperation> op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		for (int i = 0; i < op.size(); i++){
			pe.scheduleOperation(op.get(i)); 
		}
		
//		ScheduledOperation sop = storage.uploadRow(op);
		
		ArrayList<ScheduledOperation> sop = storage.uploadRowPath(op);
		
		for (int i = 0; i < op.size(); i++){
			complete(sop.get(i));
		}
		
//		try { 
//			for (int i = 0; i < op.size(); i++){
//				scheduledQueue.put(sop.get(i)); 
//			}
//		} 
//		catch (InterruptedException e) { 
//			Errors.error(e); 
//		}
		
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadColPath(ArrayList<UploadOperation> op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		for (int i = 0; i < op.size(); i++){
			pe.scheduleOperation(op.get(i));
		}
		
//		ScheduledOperation sop = storage.uploadCol(op);
		
		ArrayList<ScheduledOperation> sop = storage.uploadColPath(op);
		
		for (int i = 0; i < op.size(); i++){
			complete(sop.get(i));
		}
		
//		try { 
//			for (int i = 0; i < op.size(); i++)
//				scheduledQueue.put(sop.get(i)); 
//		} 
//		catch (InterruptedException e) { 
//			Errors.error(e); 
//		}
		
		return sop;
	}

	@Override
	public ScheduledOperation deleteObject(DeleteOperation op)
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		pe.scheduleOperation(op); // -----------
		
		ScheduledOperation sop = storage.deleteObject(op);
		
		complete(sop);
		
//		try { scheduledQueue.put(sop); } catch (InterruptedException e) { Errors.error(e); }
		
		return sop;
	}
	

	@Override
	public ScheduledOperation copyObject(CopyOperation op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		pe.scheduleOperation(op); // -----------
		
		ScheduledOperation sop = storage.copyObject(op);
		
		complete(sop);
		
//		try { scheduledQueue.put(sop); } catch (InterruptedException e) { Errors.error(e); }
		
		return sop;
	}	
	
	@Override
	public ScheduledOperation listObjects(ListOperation op) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		pe.scheduleOperation(op); // -----------
		
		ScheduledOperation sop = storage.listObjects(op);
		
		complete(sop);
		
//		try { scheduledQueue.put(sop); } catch (InterruptedException e) { Errors.error(e); }
		
		return sop;
	}

	@Override
	public void disconnect() 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		pe.disconnectCall(); // -----------
		
//		completionThread.shutdown();
//		try { completionThread.join(); } catch (InterruptedException e) { Errors.error(e); }
		
		// before we disconnect, let's get the total number of bytes used storage-side
		long bytes = storage.totalByteSize();
		pe.setStorageByteSize(bytes);
		
		storage.disconnect();
		
		pe.disconnectDone(); // -----------
		
		opened = false;
	}

	@Override
	public void cloneTo(String to) { storage.cloneTo(to); }

	@Override
	public ArrayList<ScheduledOperation> downloadCol(ArrayList<DownloadOperation> op) {
		// TODO Auto-generated method stub
		return null;
	}
}
