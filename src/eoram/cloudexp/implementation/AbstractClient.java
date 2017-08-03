package eoram.cloudexp.implementation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.crypto.CryptoProvider;
import eoram.cloudexp.interfaces.ExternalStorageInterface;
import eoram.cloudexp.interfaces.InternalClientInterface;
import eoram.cloudexp.service.GetAllRequest;
import eoram.cloudexp.service.GetColRequest;
import eoram.cloudexp.service.GetRequest;
import eoram.cloudexp.service.GetRowRequest;
import eoram.cloudexp.service.PutColRequest;
import eoram.cloudexp.service.PutRequest;
import eoram.cloudexp.service.PutRowRequest;
import eoram.cloudexp.service.ScheduledRequest;
import eoram.cloudexp.utils.EncodingUtils;

/**
 * Implements a large portion of the logic for all clients.
 * <p><p>
 * Most (if not all) clients (i.e., ORAM schemes) should be derived from this class.
 *
 */
public abstract class AbstractClient implements InternalClientInterface 
{
	protected Log log = Log.getInstance();
	protected ClientParameters clientParams = ClientParameters.getInstance();
	protected SessionState ss = SessionState.getInstance();
	
	protected ExternalStorageInterface s = null;
	
	protected File stateFile = null;
	
	protected SecureRandom rng = new SecureRandom();
	
	protected CryptoProvider cp = CryptoProvider.getInstance();
	protected EncodingUtils encodingUtils = EncodingUtils.getInstance();
	
	protected abstract void load(ObjectInputStream is) throws Exception;
	
	protected void load()
	{
		try
		{
			FileInputStream fis = new FileInputStream(stateFile);
			ObjectInputStream is = new ObjectInputStream(fis);
			
			// scheme specific load
			load(is);
			
			is.close();
		}
		catch(Exception e) { throw new RuntimeException(e); }
	}
	
	protected abstract void save(ObjectOutputStream os) throws Exception;
	
	protected void save()
	{
		try
		{
			FileOutputStream fos = new FileOutputStream(stateFile);
			ObjectOutputStream os = new ObjectOutputStream(fos);
			
			// scheme specific save
			save(os);
			
			os.flush();
			os.close();
		}
		catch(Exception e) { throw new RuntimeException(e); }
	}
	 
	/** called when open is called, after everything else **/
	protected void init(boolean reset) {}
	
	@Override
	public void open(ExternalStorageInterface storage, File state, boolean reset) 
	{
		s = storage;
		
		stateFile = state;
		
		// restore the state if needed
		if(reset == false)
		{
			load();
		}
		
		// connect to the storage
		s.connect();
		
		init(reset);
	}

	@Override
	public abstract boolean isSynchronous();

	@Override
	public abstract String getName();

	@Override
	public abstract ScheduledRequest scheduleGet(GetRequest req);
	
	@Override
	public abstract ScheduledRequest scheduleGetAll(GetAllRequest req);
	
	@Override
	public abstract ScheduledRequest scheduleGetRow(GetRowRequest req);
	
	@Override
	public abstract ScheduledRequest scheduleGetCol(GetColRequest req);

	@Override
	public abstract ScheduledRequest schedulePut(PutRequest req);
	
	@Override
	public abstract ScheduledRequest schedulePutRow(PutRowRequest req);
	
	@Override
	public abstract ScheduledRequest schedulePutCol(PutColRequest req);

	/** called when close is called, before anything else **/
	protected void shutdown() {}
	
	@Override
	public void close(String cloneStorageTo) 
	{
		shutdown();
		
		if(cloneStorageTo != null) { s.cloneTo(cloneStorageTo); } // clone if needed
		
		s.disconnect(); 
		
		save(); // save the state
	}

}
