package eoram.cloudexp.implementation;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.evaluation.PerformanceEvaluationLogger;
import eoram.cloudexp.interfaces.CompletionCallback;
import eoram.cloudexp.interfaces.ExternalClientInterface;
import eoram.cloudexp.interfaces.InternalClientInterface;
import eoram.cloudexp.interfaces.ExternalStorageInterface;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.service.*;
import eoram.cloudexp.service.Request.RequestType;
import eoram.cloudexp.utils.Errors;

/**
 * Implements an adapter between the external client interface and the internal client's logic.
 * <p><p>
 * The adapter takes care of handling performance events and completion callbacks so that the (ORAM) clients do not need to.
 */
public class ClientAdapter implements ExternalClientInterface 
{
	protected Log log = Log.getInstance();
	protected PerformanceEvaluationLogger pe = PerformanceEvaluationLogger.getInstance();
	
	protected InternalClientInterface client = null;
//	protected CompletionThread completionThread = null;
	
	protected boolean opened = false;
	
//	public class CompletionThread extends Thread
//	{
//		protected volatile boolean done = false;		
//		protected BlockingQueue<Map.Entry<ScheduledRequest, CompletionCallback>> queue = null;
//		
//		public CompletionThread(BlockingQueue<Map.Entry<ScheduledRequest, CompletionCallback>> q) { queue = q; }
//		
//		public void run()
//		{
//			Set<Map.Entry<ScheduledRequest, CompletionCallback>> pending 
//						= new HashSet<Map.Entry<ScheduledRequest, CompletionCallback>>();
//			
//			while(done == false || queue.size() > 0 || pending.size() > 0)
//			{
//				// drain stuff to pending
//				queue.drainTo(pending);
//				
//				if(pending.size() == 0) 
//				{ 
//					// poll 
//					try 
//					{
//						Map.Entry<ScheduledRequest, CompletionCallback> entry = queue.poll(5, TimeUnit.MILLISECONDS);
//						if(entry != null) { pending.add(entry); }
//					} 
//					catch (InterruptedException e1) {	e1.printStackTrace(); }
//					continue; 
//				}
//				Iterator<Map.Entry<ScheduledRequest, CompletionCallback> > iter = pending.iterator();
//				while(iter.hasNext() == true)
//				{
//					Map.Entry<ScheduledRequest, CompletionCallback> entry = iter.next();
//					ScheduledRequest sreq = entry.getKey();
//					CompletionCallback callback = entry.getValue();
//					if(sreq.isReady() == true)
//					{						
//						iter.remove(); // remove
//						
//						complete(sreq, callback); // complete the operation
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
	
//	protected BlockingQueue<Map.Entry<ScheduledRequest, CompletionCallback>> scheduledQueue 
//			= new LinkedBlockingQueue<Map.Entry<ScheduledRequest, CompletionCallback>>();
	
//	protected Set<ScheduledRequest> pendingSet = new HashSet<ScheduledRequest>();
	
	private File stateFile = null;
	
	public ClientAdapter(InternalClientInterface c)
	{
		client = c; opened = false;
//		completionThread = new CompletionThread(scheduledQueue);
	}
	
	protected void complete(ScheduledRequest sreq, CompletionCallback callback)
	{
		assert(sreq.isReady() == true);
		
		if(callback != null)
		{
			boolean success = sreq.wasSuccessful();
			if(success == true) { callback.onSuccess(sreq); }
			else { callback.onFailure(sreq); }
		}
		
		pe.completeRequest(sreq); // -----------------
		
		log.append("[CA] Just completed " + sreq.getRequest().getStringDesc(), Log.INFO);
		System.out.println("");
		System.out.println("[CA] Just completed " + sreq.getRequest().getStringDesc());
		System.out.println("");
		System.out.println("===================================================================================================================================================");
		System.out.println("===================================================================================================================================================");
	}
	
	@Override
	public void open(ExternalStorageInterface storage, File stateFile, boolean reset) 
	{
		assert(opened == false);
		
		log.append("[CA] Opening client...", Log.INFO);
		System.out.println("[CA] Opening client...");
		
//		completionThread.start(); // start the completion thread // START EVERYTHING
		
		pe.openCall(); // --------
		
		client.open(storage, stateFile, reset);
		
		pe.openDone(); // --------
		
		opened = true;
		
		this.stateFile  = stateFile; // keep a pointer on the state file for later
		
		log.append("[CA] Client opened.", Log.INFO);
		//System.out.println("[CA] Client opened.");
	}

	@Override
	public boolean isSynchronous() 
	{
		return client.isSynchronous();
	}

	@Override
	public ScheduledRequest schedule(Request req, CompletionCallback callback) 
	{
		assert(opened == true);
//		System.gc();
		log.append("[CA] Scheduling " + req.getStringDesc(), Log.INFO);
		System.out.println("");
		System.out.println("[CA] Scheduling Requests => " + req.getStringDesc());
		System.out.println("");
		System.out.println("==============================================================================");
		
		ScheduledRequest scheduled = null;
		
		if(req.getType() == RequestType.PUT)
		{
			assert(req instanceof PutRequest);
			PutRequest put = (PutRequest)req;
			
			byte[] val = put.getValue().getData(); 
			
			ClientParameters clientParams = ClientParameters.getInstance();
			if(val.length != clientParams.contentByteSize && clientParams.noSplit == false)
			{
				if(val.length > clientParams.contentByteSize) { Errors.error("Invalid PUT request data"); }
				
				val = Arrays.copyOf(val, clientParams.contentByteSize); // ensure val has the correct size
				put.setValue(new SimpleDataItem(val));
			}
		}
		else if (req.getType() == RequestType.PUTROW){
			assert(req instanceof PutRowRequest);
			PutRowRequest put = (PutRowRequest)req;
			
			byte[] val = put.getValue().getData(); 
			
			ClientParameters clientParams = ClientParameters.getInstance();
			if(val.length != clientParams.contentByteSize && clientParams.noSplit == false)
			{
				if(val.length > clientParams.contentByteSize) { Errors.error("Invalid PUT request data"); }
				
				val = Arrays.copyOf(val, clientParams.contentByteSize); // ensure val has the correct size
				put.setValue(new SimpleDataItem(val));
			}
		}
		else if((req.getType() == RequestType.PUTCOL)) {
			assert(req instanceof PutColRequest);
			PutColRequest put = (PutColRequest)req;
			
			byte[] val = put.getValue().getData(); 
			
			ClientParameters clientParams = ClientParameters.getInstance();
			if(val.length != clientParams.contentByteSize && clientParams.noSplit == false)
			{
				if(val.length > clientParams.contentByteSize) { Errors.error("Invalid PUT request data"); }
				
				val = Arrays.copyOf(val, clientParams.contentByteSize); // ensure val has the correct size
				put.setValue(new SimpleDataItem(val));
			}
		}
		
		pe.scheduleRequest(req); // -----------------
		
		if(req.getType() == RequestType.GET)
		{
			assert(req instanceof GetRequest);
			scheduled = client.scheduleGet((GetRequest)req); // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		}
		else if (req.getType() == RequestType.GETALL){
			assert(req instanceof GetAllRequest);
			scheduled = client.scheduleGetAll((GetAllRequest)req); // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		}
		else if (req.getType() == RequestType.GETROW){
			assert(req instanceof GetRowRequest);
			scheduled = client.scheduleGetRow((GetRowRequest)req); // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		}
		else if (req.getType() == RequestType.GETCOL){
			assert(req instanceof GetColRequest);
			scheduled = client.scheduleGetCol((GetColRequest)req); // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		}
		else if (req.getType() == RequestType.PUT)
		{
			assert(req instanceof PutRequest);
			scheduled = client.schedulePut((PutRequest)req); // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		}
		else if (req.getType() == RequestType.PUTROW){
			assert(req instanceof PutRowRequest);
			scheduled = client.schedulePutRow((PutRowRequest)req); // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		}
		else if (req.getType() == RequestType.PUTCOL){
			assert(req instanceof PutColRequest);
			scheduled = client.schedulePutCol((PutColRequest)req); // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		}
		
		
		if(isSynchronous() == true) // call the callback immediately
		{
			complete(scheduled, callback);
		}
		else
		{ 	// add the request to the queue
//			try { scheduledQueue.put(new AbstractMap.SimpleEntry<ScheduledRequest, CompletionCallback>(scheduled, callback)); } 
//			catch (InterruptedException e) { e.printStackTrace(); }
//			
//			pendingSet.add(scheduled); // also add it to the set
//			
//			Pollable.removeCompleted(pendingSet); // so the set doesn't get too large
		}
		
		return scheduled;
	}

	@Override
	public synchronized void waitForCompletion(Collection<ScheduledRequest> reqs) 
	{
		assert(opened == true);
//		assert(completionThread.isAlive() == true);
		
		Pollable.waitForCompletion(reqs);
	}

	@Override
	public synchronized List<ScheduledRequest> getPendingRequests() 
	{
		assert(opened == true);
//		
//		Pollable.removeCompleted(pendingSet); // so the set doesn't get too large
//		
//		List<ScheduledRequest> ret = new ArrayList<ScheduledRequest>();
//		ret.addAll(pendingSet);
//		
//		Collections.sort(ret);
		List<ScheduledRequest> ret = null;
		return ret;
		
	}

	@Override
	public void close(String cloneStorageTo) 
	{
		assert(opened == true);
		
		log.append("[CA] Closing client...", Log.INFO);
		
		long peakBytes = client.peakByteSize();
		pe.setPeakByteSize(peakBytes);
		
		pe.closeCall(); // --------
		
//		waitForCompletion(getPendingRequests());
		
//		completionThread.shutdown();
//		try { completionThread.join(); } catch (InterruptedException e) { e.printStackTrace();	}
		
		if(cloneStorageTo != null) { log.append("[CA] Cloning storage to: " + cloneStorageTo, Log.INFO); }
		client.close(cloneStorageTo);
		
		pe.closeDone(); // --------
		
		opened = false;
		
		// now that the client is closed, let's the get local byte size
		long bytes = stateFile.length();
		pe.setLocalByteSize(bytes);
		
		log.append("[CA] Client closed...", Log.INFO);		
	}

	@Override
	public String getName() { return client.getName(); }
}
