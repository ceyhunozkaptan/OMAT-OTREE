package eoram.cloudexp.schemes;


import eoram.cloudexp.data.EmptyDataItem;

import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.data.encoding.Header;
import eoram.cloudexp.implementation.AbstractClient;

import eoram.cloudexp.schemes.PathORAMBasic;

//import eoram.cloudexp.schemes.ODStree.Tree;
//import eoram.cloudexp.schemes.ODStreeCaching.Tree;
//import eoram.cloudexp.schemes.TinyOAT.Tree;
//import eoram.cloudexp.schemes.TinyOATcaching.Tree;
import eoram.cloudexp.schemes.ObliviousMatrix.Matrix;
import eoram.cloudexp.service.GetAllRequest;
import eoram.cloudexp.service.GetColRequest;
import eoram.cloudexp.service.GetRequest;
import eoram.cloudexp.service.GetRowRequest;
import eoram.cloudexp.service.PutColRequest;
import eoram.cloudexp.service.PutRequest;
import eoram.cloudexp.service.PutRowRequest;
import eoram.cloudexp.service.ScheduledRequest;
import eoram.cloudexp.utils.Errors;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;
import java.util.*;

/**
 * Implements PathORAM (see Stefanov, Emil, et al. "Path oram: An extremely simple oblivious ram protocol." ACM CCS 2013.).
 * <p><p>
 * This implementation is based on Java code obtained from authors of a follow-up work.
 * <p>
 *
 */
public class ObliviousMatrixClient extends AbstractClient
{	
	//private BitSet[] posmap = null;
	private Object[] posmap = null;
	
	ObliviousMatrix oram = null;

	
	public ObliviousMatrixClient() {}
	public ObliviousMatrixClient(SecureRandom r) { rng = r; }

	@Override
	protected void load(ObjectInputStream is) throws Exception 
	{
		// code to restore PathORAM state
		//posmap = (BitSet[])is.readObject();
		posmap = (Object[])is.readObject();
		
		int recLevels = is.readInt();
		
		//{ oram = new TinyOATcaching(rng); } //oram = new TinyOAT(rng); //oram = new ODStree(rng); //oram = new ODStreeCaching(rng); //oram = new PathORAMBasic(rng);
		if(recLevels == 0) { 
			oram = new ObliviousMatrix(rng);
		}  
		//else { oram = new PathORAMRec(clientParams.localPosMapCutoff, rng); }
		
		oram.recursiveLoad(s, is, recLevels);
	}

	@Override
	protected void save(ObjectOutputStream os) throws Exception 
	{
		// code to save PathORAM state
		os.writeObject(posmap);
		
		int recLevels = oram.getRecursionLevels();
		os.writeInt(recLevels);
		
		oram.recursiveSave(os);
	}

	
	protected void init(boolean reset) 
	{
		if(reset == true)
		{		
			long maxBlocks = clientParams.maxBlocks;
			
			assert (maxBlocks < Integer.MAX_VALUE) : "ORAM size too large: can't use as an index into an array.";
			if(maxBlocks >= Integer.MAX_VALUE) { Errors.error("ORAM size too large, not supported!"); }
			
			// DATA?
			BitSet[][] d = new BitSet[1][1];
//			for (int i = 0; i < maxBlocks; i++) { 
//				for (int k = 0; k < maxBlocks; k++)
//				d[i][k] = new BitSet(); 
//			}
			
			if(maxBlocks <= clientParams.localPosMapCutoff) { 
				oram = new ObliviousMatrix(rng);
			}  //oram = new TinyOATcaching(rng); //{oram = new TinyOAT(rng); //oram = new ODStree(rng); //oram = new ODStreeCaching(rng); //oram = new PathORAMBasic(rng);
			//else { oram = new PathORAMRec(clientParams.localPosMapCutoff, rng); }
			long t1 = System.currentTimeMillis();
			posmap = oram.initialize(s, (int)maxBlocks, clientParams.contentByteSize, d, 0);
			long t2 = System.currentTimeMillis();
			System.out.println("===================================================================================================================================================");
			System.out.println("");
			System.out.println("Storage is Initialized in " + (t2-t1)/1000.0 + " seconds");
			System.out.println("Row Position Map is set to " + posmap[0]);
			System.out.println("Col Position Map is set to " + posmap[1]);
			System.out.println("");
		}
	}

	@Override
	public boolean isSynchronous() { return true; } // PathORAM is synchronous


	@Override
	public String getName() { return "ObliviousMatrix"; }
	
	@Override
	public ScheduledRequest scheduleGet(GetRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		//try
		{
			ArrayList<Matrix.Block> res = oram.read(req.getId(), posmap, Integer.parseInt(req.getKey()), -1);
			assert(res != null);
			assert(res.get(0).data != null);
			byte[] ret = res.get(0).data.toByteArray();
			sreq.onSuccess(new SimpleDataItem(ret));
		} 
		//catch (Exception e) { sreq.onFailure(); } 
		return sreq;
	}
	
	@Override
	public ScheduledRequest scheduleGetRow(GetRowRequest req) {
		ScheduledRequest sreq = new ScheduledRequest(req);
		//try
		{
			ArrayList<Matrix.Block> res = oram.read(req.getId(), posmap, Integer.parseInt(req.getKey()), 0);
			assert(res != null);
			assert(res.get(0).data != null);
//			byte[] ret = res.get(0).data.toByteArray();
			byte[] ret = new byte[1];
			sreq.onSuccess(new SimpleDataItem(ret));
		} 
		//catch (Exception e) { sreq.onFailure(); } 
		return sreq;
	}
	@Override
	public ScheduledRequest scheduleGetCol(GetColRequest req) {
		ScheduledRequest sreq = new ScheduledRequest(req);
		//try
		{
			ArrayList<Matrix.Block> res = oram.read(req.getId(), posmap, Integer.parseInt(req.getKey()), 1);
			assert(res != null);
			assert(res.get(0).data != null);
//			byte[] ret = res.get(0).data.toByteArray();
			byte[] ret = new byte[1];// = res.get(0).data.toByteArray();
			sreq.onSuccess(new SimpleDataItem(ret));
		} 
		//catch (Exception e) { sreq.onFailure(); } 
		return sreq;
	}
	
	@Override
	public ScheduledRequest schedulePut(PutRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		try
		{
			oram.write(req.getId(), posmap, Integer.parseInt(req.getKey()), BitSet.valueOf(req.getValue().getData()), -1);
			sreq.onSuccess(new EmptyDataItem());
		} 
		catch (Exception e) { sreq.onFailure(); } 
		return sreq;
	}
	
	@Override
	public ScheduledRequest schedulePutRow(PutRowRequest req) {
		ScheduledRequest sreq = new ScheduledRequest(req);
		try
		{
			oram.write(req.getId(), posmap, Integer.parseInt(req.getKey()), BitSet.valueOf(req.getValue().getData()), 0);
			sreq.onSuccess(new EmptyDataItem());
		} 
		catch (Exception e) { sreq.onFailure(); } 
		return sreq;
	}
	@Override
	public ScheduledRequest schedulePutCol(PutColRequest req) {
		ScheduledRequest sreq = new ScheduledRequest(req);
		try
		{
			oram.write(req.getId(), posmap, Integer.parseInt(req.getKey()), BitSet.valueOf(req.getValue().getData()), 1);
			sreq.onSuccess(new EmptyDataItem());
		} 
		catch (Exception e) { sreq.onFailure(); } 
		return sreq;
	}
	
	@Override
	public long peakByteSize() 
	{
		final double bitsPerByte = 8.0;
		
		int entryByteSize = clientParams.contentByteSize + Header.getByteSize();
		long stashSize = PathORAMBasic.stashSize * entryByteSize;
		
		long effectiveN = Math.min(clientParams.maxBlocks, clientParams.localPosMapCutoff);
		
		int logMaxBlocks = (int)Math.ceil(Math.log(effectiveN)/Math.log(2.0));
		int posMapEntrySize = (int)Math.ceil(logMaxBlocks/bitsPerByte);
		long posMapSize = effectiveN * posMapEntrySize;
		
		return stashSize + posMapSize;
	}
	@Override
	public ScheduledRequest scheduleGetAll(GetAllRequest req) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	
	
}