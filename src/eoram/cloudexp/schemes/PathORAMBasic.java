package eoram.cloudexp.schemes;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.data.*;
import eoram.cloudexp.implementation.*;
import eoram.cloudexp.interfaces.*;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.schemes.PathORAMBasic.Tree.Block;
import eoram.cloudexp.schemes.TinyOAT.Tree.Bucket;
import eoram.cloudexp.service.*;
import eoram.cloudexp.service.Request.RequestType;
import eoram.cloudexp.utils.Errors;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;

/**
 * Implements basic PathORAM logic (see Stefanov, Emil, et al. "Path oram: An extremely simple oblivious ram protocol." ACM CCS 2013.).
 * <p><p>
 * This implementation is based on Java code obtained from authors of a follow-up work.
 * <p>
 *
 */
public class PathORAMBasic
{
	protected Log log = Log.getInstance();
	
	SessionState ss = SessionState.getInstance();
	ClientParameters clientParams = ClientParameters.getInstance();
	
	long totalTime = 0;
	
	final File matFile = new File("./log/" + ss.client + "_" + ss.storage + "_" + clientParams.maxBlocks + "_" + ss.experimentHash + ".perfeval2.txt");
	final File matFile2 = new File("./log/" + "matrix_experiment.perfeval2.txt");
	FileWriter fw = null;
	FileWriter fw2 = null;
	
	BufferedWriter bw = null;
	BufferedWriter bw2 = null;
	
	protected SecureRandom rnd;
	int dataSize; // measured in byte.
	int extDataSize; // measured in byte.
	static final int keyLen = 10; // length of key (to encrypt each data piece) in bytes
	static final int nonceLen = 10; // length of nonce (to encrypt each data piece) in bytes

	public static int Z = 4;
	public static int stashSize = 89; // set according to table 3 of PathORAM paper (for 80-bit security)
	
	public static int C = 1;
	//public static int C = 4; // memory reduction factor (ensure it is a power of 2)
	
	public static final boolean stashUseLS = false;
	//public static final boolean stashUseLS = true;
	
	Tree serverTree;
	int recLevel;
	
	byte[] clientKey;
	
	class Tree { //==================================================== TREE CLASS ==================================================================
		public int N; // the number of logic blocks in the tree
		public int D; // depth of the tree
//		int dataLen; // data length in bits
		
		ExternalStorageInterface storedTree;
		long treeSize;
		public Stash stash;

		Tree() { }
		/*
		 * Given input "data" (to be outsourced), initialize the server side storage and return the client side position map. 
		 * No recursion on the tree is considered. 
		 */
		private BitSet[] initialize(ExternalStorageInterface si, int maxBlocks, int dSize, BitSet[] data) 
		{
			storedTree = si;
			dataSize = dSize;
			extDataSize = dataSize + 4 + 4;
			System.out.println("dataSize=" + dataSize + "extDataSize=" + extDataSize);
			
			
			// generate a random permutation for init, so we don't get a silly stash overflow
			List<Integer> permutation = new ArrayList<Integer>();
            for (int i = 0; i < maxBlocks; i++) { 
            	permutation.add(i); }
            Collections.shuffle(permutation);
            
			// TODO
			buildTree(maxBlocks, permutation, data);
			stash = new Stash(stashSize, recLevel, stashUseLS);
			
			try{
				fw = new FileWriter(matFile, true);		
				fw2 = new FileWriter(matFile2, true);	
			}
			catch(Exception e) { Errors.error(e); }
			bw = new BufferedWriter(fw);
			bw2 = new BufferedWriter(fw2);
			
			try {
				bw2.newLine();
				bw2.write(ss.client + ", " + ss.storage + ", " + clientParams.maxBlocks + ", " + ss.experimentHash + ", ");
				bw2.flush();
				bw2.close();
			} catch (Exception e) { Errors.error(e); }
			
            // setup the position map according to the permuted blocks
 			BitSet[] posMap = new BitSet[(N + C-1) / C];	// clientPosMap[i] is the leaf label of the i-th block.
 			for (int i = 0; i < posMap.length; i++)	{ posMap[i] = new BitSet(C*D); }

 			for (int i = 0; i < N; i++) 
 			{
 				int p = i;
 				if(i < permutation.size()) { p = permutation.get(i); }
 				Utils.writePositionMap(posMap, this, i, p); 
 				
 				//{ log.append("[POB (initialize)] Block " + i + " -> leaf " + p, Log.TRACE); }
 			}

			return posMap;
		}

		private void buildTree(int maxBlocks, List<Integer> permutation, BitSet[] dataArray) 
		{
//			storedTree = new LocalStorage();
//			storedTree.initialize(null, "/tmp/Cloud" + recLevel);
			
			SessionState ss = SessionState.getInstance();
			Map<String, Request> fastInitMap = ss.fastInitMap;//////////// FAST INIT MAP !!!!!!!!!!!!!!!!!!!!!!!!!
			if(ss.fastInit == false) {  fastInitMap = null; }
			
			// set N to be the smallest power of 2 that is bigger than 'data.length'. 
			N = (int) Math.pow(2, Math.ceil(Math.log(maxBlocks)/Math.log(2))); // CEIL MAX BLOCK TO 2^x
			D = Utils.bitLength(N)-1; // BITLENGTH
			
			
			final int removeIntervalSize = 512; final double sizeFactorForSlowdown = 0.75;
			final int logIntervalSize = 8192;
			Vector<Pollable> v = new Vector<Pollable>();

			// initialize the tree
			ArrayList<UploadOperation> uploadList = new ArrayList<UploadOperation>();
			treeSize = 2*N-1;
			for (int i = 0; i < treeSize; i++) 
			{
				Bucket temp;
				if (i < treeSize/2) { 
					temp = new Bucket(new Block()); }
				else {
					if (i-N+1 < maxBlocks)
					{
						int id = permutation.indexOf(i-N+1); // make sure we are consistent with the permutation
						int label = i-N+1;
						
						BitSet data = null;
						
						String blockIdStr = "" + id;
						if(recLevel == 0 && fastInitMap != null && fastInitMap.containsKey(blockIdStr) == true)
						{
							Request req = fastInitMap.get(blockIdStr);
							Errors.verify(req.getType() == RequestType.PUT);
							PutRequest put = (PutRequest)req;
							byte[] val = put.getValue().getData();
							Errors.verify(ClientParameters.getInstance().contentByteSize <= dataSize);
							Errors.verify(val.length <= dataSize);
							
							data = BitSet.valueOf(val);
						}
						else 
						{
							if(dataArray != null)
							{
								Errors.verify(dataArray.length > id);
								data = dataArray[id];
							}
							else
							{
								long[] val = new long[1]; val[0] = id;
								data = BitSet.valueOf(val);
							}
						}
						
						temp = new Bucket(new Block(data, id, label));
						
						//{ log.append("[PathORAMBasic (BuildTree)] (R" + recLevel +") putting block " + id + " to label " + label + " (objectKey: " + recLevel + "#" + (i) + ").", Log.TRACE); }
					}
					else
						temp = new Bucket(new Block());
				}
				
				temp.encryptBlocks(); // ENCRYPT NEW TEMP BUCKET
				
				long start = System.nanoTime();
				String objectKey = recLevel + "#" + (i);
				DataItem di = new SimpleDataItem(temp.toByteArray()); // CONVERT BUCKET TO DATA
				UploadOperation upload = new UploadOperation(Request.initReqId, objectKey, di);
				uploadList.add(upload);
//				ScheduledOperation sop = storedTree.uploadObject(upload);
				
//				v.add(sop);
				
				if(i > 0 && (i % removeIntervalSize) == 0) 
				{
					Pollable.removeCompleted(v);
					
					if(v.size() >= (int)(removeIntervalSize * sizeFactorForSlowdown))
					{
						{ log.append("[PathORAMBasic (BuildTree)] Slowing down so storage can catch up...", Log.TRACE); }
						System.out.println("[PathORAMBasic (BuildTree)] Slowing down so storage can catch up...");
						int factor = (int)Math.ceil(v.size() / removeIntervalSize); if(factor > 5) { factor = 5; }
						try { Thread.sleep(factor * 5); } catch (InterruptedException e) { Errors.error(e); }
						Pollable.removeCompleted(v);
					}
				}
				
				long end = System.nanoTime();
				System.out.println("objectKey: " + objectKey + " is created!!!!!!!!!!!!!!!!!!!!!!!!!!!!! in " + (end - start) / 1000000.0 + " ms");
				
				if(i >= logIntervalSize && (i % logIntervalSize) == 0)
				{
					ArrayList<ScheduledOperation> sopList = storedTree.uploadAll(uploadList);
					v.addAll(sopList);
					uploadList.clear();
					System.gc();
					Log.getInstance().append("[PathORAMBasic (BuildTree)] Uploaded " + (i - v.size()) + " nodes so far.", Log.TRACE);
					System.out.println("[PathORAMBasic (BuildTree)] Uploaded " + (i - v.size()) + " nodes so far.");
				}
			}
			if(!uploadList.isEmpty()){
				ArrayList<ScheduledOperation> sopList = storedTree.uploadAll(uploadList);
				v.addAll(sopList);
				uploadList = null;
				sopList = null;
			}
			
			System.gc();
				
			// This waitForCompletion ensures can be used asynchronously!
			Pollable.waitForCompletion(v);
		}

		
		protected Block[] readAllBuckets (long reqId){
			Bucket[] buckets = new Bucket[(int) treeSize];
			
			Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();

//			for (int i = 0; i < buckets.length; i++) 
//			{
//				String objectKey = recLevel + "#" + (i);
//				DownloadOperation download = new DownloadOperation(reqId, objectKey);
//				ScheduledOperation sop = storedTree.downloadObject(download);
//				v.add(sop);			
//			}
			
			String objectKey = recLevel + "#" + (0);
			DownloadBulkOperation download = new DownloadBulkOperation(reqId, objectKey, 0);
			
			ScheduledOperation sop = storedTree.downloadAll(download);
			System.out.println("DOENENENENENENENENENENEN");
			v.add(sop);	
			sop = null;
			Pollable.waitForCompletion(v);
			
			ArrayList<DataItem> dataItemList = v.get(0).getDataItemList();
			v = null;
			
			for (int i = 0; i < buckets.length; i++){
				buckets[i] = new Bucket(dataItemList.get(i).getData());
				dataItemList.set(i, null);
			}
			dataItemList = null;
//			for (int i = 0; i < buckets.length; i++) { buckets[i] = new Bucket(v.get(i).getDataItem().getData()); }
					
			Block[] res = new Block[Z*buckets.length];
			
			int l = 0;
			for (int i = 0; i < buckets.length; i++) 
			{
				Bucket bkt = buckets[i];
				buckets[i] = null;
				for (int k = 0; k < bkt.blocks.length; k++)
				{
					res[l++] = new Block(bkt.blocks[k]);
				}
			}
			buckets = null;
			return res;
		}
		
		protected Block[] readBuckets(long reqId, int leafLabel) {
			
			Bucket[] buckets = getBucketsFromPath(reqId, leafLabel);
			Block[] res = new Block[Z*buckets.length];
			int i = 0;
			for (Bucket bkt : buckets) 
			{
				for (Block blk : bkt.blocks)
				{
					/*{ // debug only
						Block blk2 = new Block(blk);
						blk2.dec();
						if(blk2.isDummy() == false) { log.append("[POB (readBuckets)] Found block with id " + blk2.id, Log.TRACE); }
					}*/
					res[i++] = new Block(blk);
				}
			}
			
			return res;
		}

		private Bucket[] getBucketsFromPath(long reqId, int leaf) 
		{
			Bucket[] ret = new Bucket[D+1];
			
			Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
			ArrayList<DownloadOperation> downloadList = new ArrayList<DownloadOperation>();
			
			int temp = leaf; //((leaf+1)>>1)-1;
			for (int i = 0; i < ret.length; i++) 
			{
				String objectKey = recLevel + "#" + (temp);
				DownloadOperation download = new DownloadOperation(reqId, objectKey);
				downloadList.add(download);
//				ScheduledOperation sop = storedTree.downloadObject(download);
//				v.add(sop);
				
				// debug only
				//{ log.append("[POB (getBucketsFromPath)] reading down to leaf " + leaf + " (" + (leaf - (N-1))  + ") objectKey: " + objectKey, Log.ERROR); }
				
				if (temp > 0) { temp = ((temp+1)>>1)-1; }
			}
			
			long start = System.nanoTime();
			ArrayList <ScheduledOperation> sop = storedTree.downloadPath(downloadList);
			long end = System.nanoTime();
			System.out.println("[PO (getBucketsFromPath)] Read from Root to Leaf (" + (leaf - (N-1))  + ") in " + (end - start) / 1000000.0 + " ms");
			
			v.addAll(sop);
			
			Pollable.waitForCompletion(v);
			for (int i = 0; i < ret.length; i++) { ret[i] = new Bucket(v.get(i).getDataItem().getData()); }
			return ret;
		}


		class Block { // ==================================== BLOCK CLASS ====================================================================
			BitSet data;
			int id; // range: 0...N-1; // DUMMY BLOCKS ARE 'N'
			int treeLabel;
			
			private byte[] r; 
			
			public Block(Block blk) {
				assert (blk.data != null) : "no BitSet data pointers is allowed to be null.";
				try { data = (BitSet) blk.data.clone(); } 
				catch (Exception e) { e.printStackTrace(); System.exit(1); }
				id = blk.id;
				treeLabel = blk.treeLabel;
				r = blk.r;
			}
			
			Block(BitSet data, int id, int label) {
				assert (data != null) : "Null BitSet data pointer.";
				this.data = data;
				this.id = id;
				this.treeLabel = label;
			}
			
			public Block() {
				data = new BitSet(dataSize*8);
				id = N; // id == N marks a dummy block, so the range of id is from 0 to N, both ends inclusive. Hence the bit length of id is D+1.
				treeLabel = 0;
			}
			
			public Block(byte[] bytes) {
				byte[] bs = new byte[dataSize];
				ByteBuffer bb = ByteBuffer.wrap(bytes); //.order(ByteOrder.LITTLE_ENDIAN);
				bb = bb.get(bs);
				data = BitSet.valueOf(bs);
				id = bb.getInt();
				treeLabel = bb.getInt();
				r = new byte[nonceLen];
				bb.get(r);
			}
			
			public Block(byte[] bytes, boolean stash) {
				byte[] bs = new byte[dataSize];
				ByteBuffer bb = ByteBuffer.wrap(bytes); //.order(ByteOrder.LITTLE_ENDIAN);
				bb = bb.get(bs);
				data = BitSet.valueOf(bs);
				id = bb.getInt();
				treeLabel = bb.getInt();
			}
			
			public boolean isDummy() {
				assert (r == null) : "isDummy() was called on encrypted block";
				return id == N;
			}
			
			public void erase() { id = N; treeLabel = 0; }
			
			
			public byte[] toByteArray() {
				ByteBuffer bb = ByteBuffer.allocate(extDataSize+nonceLen);

				// convert data into a byte array of length 'dataSize'
				byte[] d = new byte[dataSize];
				byte[] temp = data.toByteArray();
				for (int i=0; i<temp.length; i++) {
					d[i] = temp[i];
			    }
				
				bb.put(d);
				
				bb.putInt(id).putInt(treeLabel);
				
				bb.put((r == null) ? new byte[nonceLen] : r);
					
				return bb.array();
			}
			
			public String toString() {return Arrays.toString(toByteArray());}
			
			private void enc() {
				r = Utils.genPRBits(rnd, nonceLen);
				mask();
			}
			
			private void dec() { 
				mask();
				r = null;
			}
			
			private void mask() {
				byte[] mask = new byte[extDataSize];
				try {
					MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
					int hashLength = 20;
					int i = 0;
					for (; (i+1)*hashLength < extDataSize; i++) {
						sha1.update(clientKey);
						sha1.update(r);
						sha1.update(ByteBuffer.allocate(4).putInt(i));
						System.arraycopy(sha1.digest(), 0, mask, i*hashLength, hashLength);
					}
					sha1.update(clientKey);
					sha1.update(r);
					sha1.update(ByteBuffer.allocate(4).putInt(i));
					System.arraycopy(sha1.digest(), 0, mask, i*hashLength, extDataSize-i*hashLength);

					BitSet dataMask = BitSet.valueOf(Arrays.copyOfRange(mask, 0, dataSize));
					data.xor(dataMask);
					id ^= ByteBuffer.wrap(mask, dataSize, 4).getInt();
					treeLabel ^= ByteBuffer.wrap(mask, dataSize+4, 4).getInt();
				} catch (Exception e) { 
					e.printStackTrace(); 
					System.exit(1); 
				}
			}
		}
		
		class Bucket { //============================================== BUCKET CLASS =====================================
			Block[] blocks = new Block[Z];
			
			Bucket(Block b) { // BUCKET WITH Block b
				assert (b != null) : "No null block pointers allowed.";
				blocks[0] = b;
				for (int i = 1; i < Z; i++) // FILL OTHER BLOCK WITH DUMMY
					blocks[i] = new Block();
			}
			
			Bucket(byte[] array) {
				ByteBuffer bb = ByteBuffer.wrap(array);
				byte[] temp = new byte[extDataSize+nonceLen];
				for (int i = 0; i < Z; i++) {
					bb.get(temp);
					blocks[i] = new Block(temp);
				}
			}
			
			public byte[] toByteArray() {
				ByteBuffer bb = ByteBuffer.allocate(Z * (extDataSize+nonceLen));
				for (Block blk : blocks)
					bb.put(blk.toByteArray());
				return bb.array();
			}

			void encryptBlocks() { // ENCRYPT 1 BUCKET
				for (Block blk : blocks)
					blk.enc();
			}
		}
		
		class Stash //=================================================== STASH CLASS ======================================================
		{
			LocalStorage ls = null;
			List<Block> blocks = null;
			int size = 0; // in blocks
			
			public Stash(int size, int recLevel, boolean useLS) 
			{
				this.ls = null;
				this.blocks = null;
				this.size = size;
				
				if(useLS == true)
				{		
					ls = new LocalStorage("/tmp/Local" + recLevel, true);
					ls.connect();
	
					for (int i = 0; i < size; i++) 
					{
						String objectKey = recLevel + "#" + (i);
						DataItem di = new SimpleDataItem(new Block().toByteArray());
						UploadOperation upload = new UploadOperation(Request.initReqId, objectKey, di);
						ScheduledOperation sop = ls.uploadObject(upload);
						sop.waitUntilReady();
					}
				}
				else
				{
					// use list of blocks (in memory)
					blocks = new ArrayList<Block>();
				}
			}

			public void save(ObjectOutputStream os) throws IOException
			{
				os.writeInt(size);
				os.writeInt(recLevel);
				
				boolean useLS = (ls != null);
				os.writeBoolean(useLS);
								
				if(useLS == true)
				{
					for (int i = 0; i < size; i++) 
					{
						String objectKey = recLevel + "#" + (i);
						DownloadOperation download = new DownloadOperation(Request.initReqId, objectKey);
						ScheduledOperation sop = ls.downloadObject(download);
						sop.waitUntilReady();
						
						byte[] data = sop.getDataItem().getData();
						
						/*{ // tmp debug
							Block blk = new Block(data, true);
							if(blk.isDummy() == false) 
							{ log.append("[POB (saveStash)] Saving block with id " + blk.id, Log.TRACE); }
						}*/
						
						os.writeInt(data.length);
						os.write(data);
					}
				}
				else
				{
					os.writeInt(blocks.size());
					for(int i=0; i<blocks.size(); i++)
					{
						Block blk = blocks.get(i);
						byte[] data = blk.toByteArray();
						

						/*{ // tmp debug
							if(blk.isDummy() == false) 
							{ log.append("[POB (saveStash)] Saving block with id " + blk.id, Log.TRACE); }
						}*/
						
						os.writeInt(data.length);
						os.write(data);
					}
				}
			}
			
			public Stash(ObjectInputStream is) throws IOException
			{
				this.ls = null;
				this.blocks = null;
				size = is.readInt();
				
				int tmpRecLevel = is.readInt();
				
				boolean useLS = is.readBoolean();
				
				if(useLS == true)
				{
					ls = new LocalStorage("/tmp/Local" + tmpRecLevel, true);
					ls.connect();
					
					for (int i = 0; i < size; i++) 
					{
						int byteSize = is.readInt();
						byte[] data = new byte[byteSize];
						is.readFully(data);
						
						/*{ // tmp debug
							Block blk = new Block(data, true);
							if(blk.isDummy() == false) 
							{ log.append("[POB (loadStash)] Loaded block with id " + blk.id, Log.TRACE); }
						}*/
						
						String objectKey = recLevel + "#" + (i);
						DataItem di = new SimpleDataItem(data);
						UploadOperation upload = new UploadOperation(Request.initReqId, objectKey, di);
						ScheduledOperation sop = ls.uploadObject(upload);
						sop.waitUntilReady();
					}
				}
				else
				{
					blocks = new ArrayList<Block>();
					
					int s = is.readInt();
					for(int i=0; i<s; i++)
					{
						int byteSize = is.readInt();
						byte[] data = new byte[byteSize];
						is.readFully(data);
						
						Block blk = new Block(data, true);
						
						/*{ // tmp debug
							if(blk.isDummy() == false) 
							{ log.append("[POB (loadStash)] Loaded block with id " + blk.id, Log.TRACE); }
						}*/
						
						blocks.add(blk);
					}
				}
			}
		}

		protected void save(ObjectOutputStream os) throws IOException
		{
			os.writeInt(N);
			os.writeInt(D);

			os.writeLong(treeSize);
			
			stash.save(os);
		}
		
		public Tree(ExternalStorageInterface si, ObjectInputStream is)  throws IOException
		{
			storedTree = si;
			N = is.readInt();
			D = is.readInt();
			
			treeSize = is.readLong();
			
			try{	
				fw2 = new FileWriter(matFile2, true);	
			}
			catch(Exception e) { Errors.error(e); }
			bw2 = new BufferedWriter(fw2);
			
			try {
				bw2.newLine();
				bw2.write(ss.client + ", " + ss.storage + ", " + clientParams.maxBlocks + ", " + ss.experimentHash + ", ");
				bw2.flush();
				bw2.close();
			} catch (Exception e) { Errors.error(e); }
			
			stash = new Stash(is);
		}
	}
	
	public PathORAMBasic (SecureRandom rand) 
	{
		rnd = rand;
		clientKey = Utils.genPRBits(rnd, keyLen);
	}
	
	/*
	 * Each data has 'unitB' bytes.
	 * 'data' will be intact since the real block has a clone of each element in 'data'.
	 */
	public BitSet[] initialize(ExternalStorageInterface si, int maxBlocks, int unitB, BitSet[] data, int recLevel) {
		assert(maxBlocks < (~(1<<63))) : "Too many blocks in a tree.";
		
		this.recLevel = recLevel;
		int nextB = unitB;
		serverTree = new Tree();////////////////////SERVER TREE
		BitSet[] posMap = serverTree.initialize(si, maxBlocks, nextB, data);////////////////////POSITION MAP

		return posMap;
	}

	public enum OpType {Read, Write};
	protected Tree.Block access(long reqId, BitSet[] posMap, OpType op, int a, BitSet data)
	{
		Tree tr = serverTree;
		
		try{
			fw = new FileWriter(matFile, true);		
			fw2 = new FileWriter(matFile2, true);	
		}
		catch(Exception e) { Errors.error(e); }
		bw = new BufferedWriter(fw);
		
		long start = System.nanoTime();
		int leafLabel = tr.N-1 + Utils.readPositionMap(posMap, tr, a);
		int newLabel = rnd.nextInt(tr.N);
		System.out.println("[PO (access)] Search for ID " + a + " would start from the root in the Path down to leaf " + leafLabel + " (label: " + (leafLabel - (tr.N-1)) + ") or in the Stash (new label: " + newLabel + ").");
		Tree.Block[] blocks = tr.readBuckets(reqId, leafLabel);
		long end = System.nanoTime();
		try {
			bw.write((int) ((end - start) / 1000000.0)+", " );
			bw.flush();
			bw.close();
			totalTime = (end - start);
		} catch (Exception e) { Errors.error(e); }
		System.out.println("==============================================================================");
		System.out.println("ALL DOWNLOAD TOOK " + (end - start) / 1000000.0 + " ms");
		System.out.println("==============================================================================");
		System.out.println("");
		System.out.println("DOWNLOAD IS DONE. \nBLOCKS WILL BE DECRYPTED & SEARCHED. \nNEW LABELS WILL BE GIVEN");
		System.out.println("");
		System.out.println("==============================================================================");
		
		
		Utils.writePositionMap(posMap, tr, a, newLabel); //(u % s.C)*s.D, newlabel, s.D);
		
		
		
		// debug only
		//{ log.append("[POB (access)] (R" + recLevel + ") Block with id " + a + " should be in the path down to leaf " + leafLabel + " (label: " + (leafLabel - (tr.N-1)) + ") or in the stash (new label: " + newLabel + ").", Log.TRACE); }
		
		return rearrangeBlocksAndReturn(reqId, op, a, data, leafLabel, blocks, newLabel);
	}

	protected Tree.Block rearrangeBlocksAndReturn(long reqID, OpType op, int a, BitSet data, int leafLabel, Tree.Block[] blocks, int newlabel)
	{
		Tree tr = serverTree;
		Tree.Block res = null;
		Tree.Block[] union = new Tree.Block[tr.stash.size+blocks.length];
		
		try{
			fw = new FileWriter(matFile, true);		
			fw2 = new FileWriter(matFile2, true);	
		}
		catch(Exception e) { Errors.error(e); }
		bw = new BufferedWriter(fw);
		bw2 = new BufferedWriter(fw2);
		
		Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
		
		if(tr.stash.ls != null) // use LS
		{
			for (int i = 0; i < tr.stash.size; i++) 
			{
				String objectKey = recLevel+"#"+(i);
				DownloadOperation download = new DownloadOperation(reqID, objectKey);
				ScheduledOperation sop = tr.stash.ls.downloadObject(download);
				v.add(sop);
			}
			Pollable.waitForCompletion(v);
			for (int i = 0; i < tr.stash.size; i++) { union[i] = tr.new Block(v.get(i).getDataItem().getData(), true);}
		}
		else
		{
			for (int i = 0; i < tr.stash.size; i++)
			{
				Tree.Block blk = null;
				if(i < tr.stash.blocks.size()) { blk = tr.stash.blocks.get(i); }
				else { blk = tr.new Block(); }
				union[i] = blk;
			}
		}
		
		for (int i = 0; i < blocks.length; i++) { union[i+tr.stash.size] = tr.new Block(blocks[i]); }
		
		long startFind = System.nanoTime();
		
		for (Tree.Block blk : union)
		{
			if (blk.r != null) // when r == null, the block comes from the stash, already decrypted.
			{ blk.dec(); }
		}
		for (int i = 0; i < union.length; i++)
		{
			Tree.Block blk = union[i];
			if (blk.id == a) {
				res = tr.new Block(blk);
				System.out.println("SUCCESS: Block is retrieved. ID= " + a);
				blk.treeLabel = newlabel;
				System.out.println("ID " + a + " was in Leaf=" + leafLabel + " and will be put in Leaf-" + blk.treeLabel);	
				if (op == OpType.Write) {
					blk.data = data;
				}
			}
		}
		
		
		
		/*
		{ // debug only
			for (int i = 0; i < tr.stash.size; i++) 
			{ 
				Tree.Block blk = union[i]; 
				if(blk.isDummy() == false) 
				{ log.append("[POB (rearrangeBlocksAndReturn)] Stash contains block with id " + blk.id, Log.TRACE); }
			}
			for (int i = 0; i < blocks.length; i++) 
			{ 
				Tree.Block blk = union[i+tr.stash.size]; 
				if(blk.isDummy() == false) { log.append("[POB (rearrangeBlocksAndReturn)] Path down to label " + (leafLabel - (tr.N-1)) + " contain block with id " + blk.id, Log.TRACE); }
			}
		}*/
		if(res == null)
		{
			Errors.error("[POB (rearrangeBlocksAndReturn)] Couldn't find block with id " + a + " in the path down to label " + (leafLabel - (tr.N-1))+ " or in the stash.");
		}
		
		long endFind = System.nanoTime();
		
		try {
			bw.write((int) ((endFind - startFind) / 1000000.0)+", " );
			totalTime += (endFind - startFind);
		} catch (Exception e) { Errors.error(e); }
		System.out.println("==============================================================================");
		System.out.println("ALL DECRYPTION & SEARCH TOOK " + (endFind - startFind) / 1000000.0 + " ms");
		System.out.println("==============================================================================");
		System.out.println("");
		System.out.println("DECRYPTION & SEARCH IS DONE. \nNEW LABELS ARE GIVEN. \nRE-ENCRYPTION & EVICTION WILL START");
		System.out.println("");
		System.out.println("==============================================================================");
		
		List<Tree.Block> unionList = Arrays.asList(union);
		Collections.shuffle(unionList, rnd);
		union = unionList.toArray(new Tree.Block[0]);
		
		/** // this code is equivalent to (and easier to follow than) the piece right below
		 
		v.clear();
		Set<Integer> pathSet = getPathString(leafLabel); System.out.println(pathSet);
		Map<Integer, Tree.Bucket> pathMap = new TreeMap<Integer, Tree.Bucket>();
		
		for(int node : pathSet) { pathMap.put(node, tr.new Bucket(tr.new Block())); }
		
		int failedToEvictCount = 0;
		for (int j = 0; j < union.length; j++) 
		{
			Tree.Block thisBlock = union[j];
			if (thisBlock.isDummy() == false)
			{ 
				Set<Integer> assignedPathSet = getPathString(thisBlock.treeLabel+tr.N-1);
				
				Set<Integer> intersection = new HashSet<Integer>(pathSet);
				intersection.retainAll(assignedPathSet);
				
				int max = Collections.max(intersection);
				
				//System.out.println("\t" + intersection + " -> " + max + "\t(" + assignedPathSet + ")");
				
				Tree.Bucket bucket = pathMap.get(max);
				assert(bucket != null);

				boolean failedToEvict = true;
				for(int k=0; k < Z; k++) 
				{
					if(bucket.blocks[k].isDummy() == true)
					{
						bucket.blocks[k] = tr.new Block(thisBlock);
						thisBlock.erase(); 
						failedToEvict = false;
						break;
					}
				}
				
				if(failedToEvict == true) { failedToEvictCount++; }
			}
		}
		
		//System.out.println("failedToEvictCount: " + failedToEvictCount);
		
		for(int node : pathMap.keySet())
		{		
			Tree.Bucket bucket = pathMap.get(node); assert(bucket != null);
			bucket.encryptBlocks();
		
			String objectKey = recLevel + "#" + node;
			DataItem di = new SimpleDataItem(bucket.toByteArray());
			UploadOperation upload = new UploadOperation(reqID, objectKey, di);
			ScheduledOperation sop = tr.storedTree.uploadObject(upload);
			
			//System.out.print("objectKey: " + objectKey + " ");
			
			v.add(sop);
		}
		Pollable.waitForCompletion(v); v.clear(); // wait and clear
		**/
		
		System.out.println("==============================================================================");
		System.out.println("");
		System.out.println("RE-ENCRYPTION & EVICTION IS STARTED");
		System.out.println("");
		System.out.println("==============================================================================");
		
		long startEvict = System.nanoTime();
		v.clear();
		ArrayList<UploadOperation> uploadList = new ArrayList<UploadOperation>();
		for (int i = tr.D+1; i > 0; i--) 
		{
			int prefix = Utils.iBitsPrefix(leafLabel+1, tr.D+1, i);
			
			String objectKey = recLevel + "#" + ((prefix>>(tr.D+1-i))-1);
			
			Tree.Bucket bucket = tr.new Bucket(tr.new Block());
			for (int j = 0, k = 0; j < union.length && k < Z; j++) 
			{
				if (!union[j].isDummy())
				{ 
					int jprefix = Utils.iBitsPrefix(union[j].treeLabel+tr.N, tr.D+1, i);
					
					if(prefix == jprefix) 
					{
						bucket.blocks[k++] = tr.new Block(union[j]);
						
						// debug only
						//{ log.append("[POB (rearrangeBlocksAndReturn)] Block with id " + union[j].id + " will be re-written in bucket on the path to label " + union[j].treeLabel + " (objectKey: " + objectKey + ")", Log.TRACE); }
						
						union[j].erase();
					}
				}
			}		
			
			/*for(int k=0; k<Z; k++) // debug only
			{
				Tree.Block blk = bucket.blocks[k];
				if(blk.isDummy() == false)
				{
					{ log.append("[POB (rearrangeBlocksAndReturn)] Bucket " + objectKey + " contains block with id " + blk.id + " with label " + blk.treeLabel, Log.TRACE); }
				}
			}*/
			
			bucket.encryptBlocks();
			
			DataItem di = new SimpleDataItem(bucket.toByteArray());
			UploadOperation upload = new UploadOperation(reqID, objectKey, di);
			uploadList.add(upload);
//			ScheduledOperation sop = tr.storedTree.uploadObject(upload);
			
//			v.add(sop);
		}
		long endEvict = System.nanoTime();
		System.out.println("Select & Encrypt = " + (endEvict-startEvict)/ 1000000.0 + " ms");
		
		long startUpl = System.nanoTime();
		ArrayList<ScheduledOperation> sopList = tr.storedTree.uploadPath(uploadList);
		v.addAll(sopList);		
		Pollable.waitForCompletion(v); v.clear(); // wait and clear
		long endUpl = System.nanoTime();
		System.out.println("Upload = " + (endUpl-startUpl)/ 1000000.0 + " ms");
		
		
		
		if(tr.stash.ls == null) { tr.stash.blocks.clear(); } // clear stash in memory
		
		
		// put the rest of the blocks in 'union' into the 'stash'
		int j = 0, k = 0;
		for (; j < union.length && k < tr.stash.size; j++) 
		{
			if (union[j].isDummy() == false) 
			{
				if(tr.stash.ls != null) // use LS
				{
					String objectKey = recLevel + "#" + (k);
					DataItem di = new SimpleDataItem(union[j].toByteArray());
					UploadOperation upload = new UploadOperation(reqID, objectKey, di);
					ScheduledOperation sop = tr.stash.ls.uploadObject(upload);
					sop.waitUntilReady();
				}
				else
				{
					tr.stash.blocks.add(tr.new Block(union[j]));
				}
				
				k++;

				union[j].erase();
			}
		}
		
		
		try {
			bw.write((int) ((endEvict - startEvict) / 1000000.0)+", " );
			bw.write((int) ((endUpl-startUpl)/ 1000000.0)+"");
			bw.newLine();
			bw.flush();
			bw.close();
			totalTime += (endEvict-startEvict);
			totalTime += (endUpl-startUpl);
			bw2.write((int)(totalTime/1000000.0) + ", " );
			bw2.flush();
			bw2.close();
			
		} catch (Exception e) { Errors.error(e); }
		System.out.println("==============================================================================");
		System.out.println("ALL RE-ENCRYPTION & EVICTION TOOK " + (endEvict - startEvict) / 1000000.0 + " ms"); 
		System.out.println("==============================================================================");
		System.out.println("");
		System.out.println("CURRENT STASH SIZE = " + k);
		System.out.println("Position Map is set to " + newlabel);
		System.out.println("");
		System.out.println("==============================================================================");
		
		
		if (k == tr.stash.size) 
		{
			for (; j < union.length; j++)
			{ assert (union[j].isDummy()) : "Stash is overflown: " + tr.stash.size; }	
		}
		
		if(tr.stash.ls != null) // use LS
		{
			while (k < tr.stash.size) 
			{
				String objectKey = recLevel + "#" + (k);
				DataItem di = new SimpleDataItem(tr.new Block().toByteArray());
				UploadOperation upload = new UploadOperation(reqID, objectKey, di);
				ScheduledOperation sop = tr.stash.ls.uploadObject(upload);
				sop.waitUntilReady();
		
				k++;
			}
		}
		
		return res;
	}
	
	private Set<Integer> getPathString(int leaf) 
	{
		Set<Integer> nodesList = new TreeSet<Integer>();
		int temp = leaf;
		while(temp >= 0)
		{
			nodesList.add(temp);
			temp = ((temp+1)>>1)-1;
		}
		return nodesList;
	}

	Tree.Block read(long reqID, BitSet[] pm, int i) { return access(reqID, pm, OpType.Read, i, null); }
	
	Tree.Block write(long reqID, BitSet[] pm, int i, BitSet d) { return access(reqID, pm, OpType.Write, i, d); }

	
	BitSet[] readAll(long reqID, BitSet[] posmap, int a){
		Tree tr = serverTree;
		double sizeFactorForSlowdown = 0.75;
		int logIntervalSize = 50;
		int removeIntervalSize = 100; 
		
		try{
			fw = new FileWriter(matFile, true);		
			fw2 = new FileWriter(matFile2, true);	
		}
		catch(Exception e) { Errors.error(e); }
		bw = new BufferedWriter(fw);
		bw2 = new BufferedWriter(fw2);
		
		
		long start = System.nanoTime();
		Tree.Block[] blocks = tr.readAllBuckets(reqID);	
		ArrayList<Tree.Block> retrieved = new ArrayList<Tree.Block>();
		long end = System.nanoTime();
		System.out.println("All Buckets Downloaded in " + (end - start) / 1000000.0 + " ms");
		try {
			bw.write((int) ((end - start) / 1000000.0)+", " );
			totalTime = (end - start);
		} catch (Exception e) { Errors.error(e); }
		
		System.gc();
		
		start = System.nanoTime();
		ArrayList<Block> unionList = new ArrayList<Block>();	
		for (int i = 0; i < tr.stash.size; i++)
		{
			Tree.Block blk = null;
			if(i < tr.stash.blocks.size()) { 
				blk = tr.stash.blocks.get(i); 
			}
			else { 
				blk = tr.new Block(); 
			}
			unionList.add(blk);
		}
		
		tr.stash.blocks.clear();
		
		for (int i = 0; i < blocks.length; i++) { 
			unionList.add(tr.new Block(blocks[i])); 
			blocks[i] = null;
		}
		blocks = null;
		
		for (int i = 0; i < unionList.size(); i++)
		{
			if (unionList.get(i).r != null) // when r == null, the block comes from the stash, already decrypted.
			{ unionList.get(i).dec(); }
		}
		
		for (int i = 0; i < unionList.size(); i++)
		{
			if(!unionList.get(i).isDummy()){
				retrieved.add(unionList.get(i));
				unionList.set(i, null);
			}
				
		}
		end = System.nanoTime();
		System.out.println("All Data is Retrieved & Decrypted in " + (end - start) / 1000000.0 + " ms");
		try {
			bw.write((int) ((end - start) / 1000000.0)+", " );
			totalTime += (end - start);
		} catch (Exception e) { Errors.error(e); }
		
		unionList = null;
		System.gc();
		System.out.println("unionList is cleared!!!!!!!!!!!!!!!!!!!!!!");
		
		start = System.nanoTime();
		List<Integer> permutation = new ArrayList<Integer>();
		int maxBlocks = retrieved.size();
		
        for (int i = 0; i < maxBlocks; i++) { 
        	permutation.add(i); 
    	}
        Collections.shuffle(permutation);
		
        Vector<Pollable> v = new Vector<Pollable>();
              
        ArrayList<UploadOperation> uploadList = new ArrayList<UploadOperation>();
        for (int i = 0; i < tr.treeSize; i++) 
		{
			Tree.Bucket temp;
			String objectKey = null;
			if (i < tr.treeSize/2) { 
				temp = tr.new Bucket(tr.new Block()); 
				objectKey = recLevel + "#" + (i);
			}
			else {
				if (i-tr.N+1 < maxBlocks)
				{
					int id = retrieved.get(i-tr.N+1).id;
					retrieved.get(i-tr.N+1).treeLabel = permutation.get(id);
//							permutation.indexOf(i-tr.N+1); // make sure we are consistent with the permutation
//					int label = i-tr.N+1;
							
//					temp = tr.new Bucket(tr.new Block(new BitSet(), id, label));
					temp = tr.new Bucket(retrieved.get(i-tr.N+1));
					retrieved.set(i-tr.N+1, null);
					objectKey = recLevel + "#" + (permutation.get(id)+tr.N-1);
//					System.out.println("NodeID " + id + "Leaf " + (permutation.get(id)+tr.N-1));
					//{ log.append("[PathORAMBasic (BuildTree)] (R" + recLevel +") putting block " + id + " to label " + label + " (objectKey: " + recLevel + "#" + (i) + ").", Log.TRACE); }
				}
				else{
					temp = tr.new Bucket(tr.new Block());
					objectKey = recLevel + "#" + (i);
				}
					
			}
			
			temp.encryptBlocks(); // ENCRYPT NEW TEMP BUCKET
			
			long start23 = System.nanoTime();
//			String objectKey = recLevel + "#" + (i);
			DataItem di = new SimpleDataItem(temp.toByteArray()); // CONVERT BUCKET TO DATA
			UploadOperation upload = new UploadOperation(reqID, objectKey, di);
			uploadList.add(upload);
			
			
			if(i > 0 && (i % removeIntervalSize) == 0) 
			{
				Pollable.removeCompleted(v);
				
				
				if(v.size() >= (int)(removeIntervalSize * sizeFactorForSlowdown))
				{
					{ log.append("[PathORAMBasic (BuildTree)] Slowing down so storage can catch up...", Log.TRACE); }
					System.out.println("[PathORAMBasic (BuildTree)] Slowing down so storage can catch up...");
					int factor = (int)Math.ceil(v.size() / removeIntervalSize); if(factor > 5) { factor = 5; }
					try { Thread.sleep(factor * 5); } catch (InterruptedException e) { Errors.error(e); }
					Pollable.removeCompleted(v);
				}
			}
			
//			long end = System.nanoTime();
//			System.out.println("objectKey: " + objectKey + " is created!!!!!!!!!!!!!!!!!!!!!!!!!!!!! in " + (end - start) / 1000000.0 + " ms");		
			
//			if(i > 0 && (i % (tr.treeSize/logIntervalSize)) == 0)
//			{
////				Log.getInstance().append("[PathORAMBasic (BuildTree)] Uploaded " + (i - v.size()) + " nodes so far.", Log.TRACE);
////				System.out.println("[PathORAMBasic (BuildTree)] Uploaded " + (i - v.size()) + " nodes so far.");
//				ArrayList<ScheduledOperation> sopList = tr.storedTree.uploadAll(uploadList);
//				v.addAll(sopList);
//				uploadList.clear();
//				Pollable.waitForCompletion(v);			
//			}
			
//			ScheduledOperation sop = tr.storedTree.uploadObject(upload);
			
//			v.add(sop);
						
//			long end23 = System.nanoTime();
//			System.out.println("objectKey: " + objectKey + " is created!!!!!!!!!!!!!!!!!!!!!!!!!!!!! in " + (end23 - start23) / 1000000.0 + " ms");
			
		}
        end = System.nanoTime();
        retrieved = null;
        System.gc();
		System.out.println("All Data is Re-Encrypted in " + (end - start) / 1000000.0 + " ms");
		try {
			bw.write((int) ((end - start) / 1000000.0)+", " );
			totalTime += (end - start);
		} catch (Exception e) { Errors.error(e); }
		
		
        start = System.nanoTime();
        if(!uploadList.isEmpty()){
        	ArrayList<ScheduledOperation> sopList = tr.storedTree.uploadAll(uploadList);
        	uploadList = null;
        	v.addAll(sopList);
    		sopList = null;
        }
        Pollable.waitForCompletion(v);
        end = System.nanoTime();
		System.out.println("All Data is Uploaded in " + (end - start) / 1000000.0 + " ms");
		try {
			bw.write((int) ((end - start) / 1000000.0)+"" );
			bw.newLine();
			bw.flush();
			bw.close();
			totalTime += (end - start);
			bw2.write((int)(totalTime/1000000.0) + ", " );
			bw2.flush();
			bw2.close();
		} catch (Exception e) { Errors.error(e); }
		
		v = null;
		unionList = null;
		System.gc();
		
		BitSet[] posMap = new BitSet[(tr.N + C-1) / C];	// clientPosMap[i] is the leaf label of the i-th block.
		for (int i = 0; i < posMap.length; i++)	{ posMap[i] = new BitSet(C*tr.D); }

		for (int i = 0; i < tr.N; i++) 
		{
			int p = i;
			if(i < permutation.size()) { p = permutation.get(i); }
			Utils.writePositionMap(posMap, tr, i, p); 			
		}
		
		
		
		return posMap;
	}
	
	static class Utils {
		/*
		 * Given 'n' that has 'w' bits, output 'res' whose most significant 'i' bits are identical to 'n' but the rest are 0.
		 */
		static int iBitsPrefix(int n, int w, int i) {
			return (~((1<<(w-i)) - 1)) & n;
		}
		
		/*
		 * keys, from 1 to D, are sorted in ascending order
		 */
		static void refineKeys(int[] keys, int d, int z) {
			int j = 0, k = 1;
			for (int i = 1; i <= d && j < keys.length; i++) {
				while (j < keys.length && keys[j] == i) {
					keys[j] = (i-1)*z + k;
					j++; k++;
				}
				if (k <= z) 
					k = 1;
				else
					k -= z;
			}
		}
		
		
//		private static <T> void swap(T[] arr, int i, int j) {
//			T temp = arr[i];
//			arr[i] = arr[j];
//			arr[j] = temp;
//		}
		
		static void writePositionMap(BitSet[] map, Tree st, int index, int val) {
			int base = (index % C) * st.D;
			writeBitSet(map[index/C], base, val, st.D);
		}
		
		static BitSet writeBitSet(BitSet map, int base, int val, int d) {
			for (int i = 0; i < d; i++) {
				if (((val>>i) & 1) == 1)
					map.set(base + i);
				else
					map.clear(base + i);
			}
			
			return map;
		}
		
		static BitSet writeBitSet(BitSet map, int base, BitSet val, int d) {
			for (int i = 0; i < d; i++) {
				if (val.get(i))
					map.set(base + i);
				else
					map.clear(base + i);
			}
			
			return map;
		}
		
		static int readPositionMap(BitSet[] map, Tree st, int index) {
			int base = fastMod(index, C) * st.D;
			int mapIdx = fastDivide(index, C);
			if(mapIdx >= map.length) { Errors.error("Coding FAIL!"); }
			return readBitSet(map[mapIdx], base, st.D);
		}
		
		static int readBitSet(BitSet map, int base, int d) {
			int ret = 0;
			for (int i = 0; i < d; i++) {
				if (map.get(base + i) == true)
					ret ^= (1<<i);
			}
			return ret;
		}
		
		/* 
		 * n has to be a power of 2.
		 * Return the number of bits to denote n, including the leading 1.
		 */
		static int bitLength(int n) {
			if (n == 0) 
				return 1;
			
			int res = 0;
			do {
				n = n >> 1;
				res++;
			} while (n > 0);
			return res;
		}
		
		static int fastMod(int a, int b) {
			// b is a power of 2
			int shifts = (int) (Math.log(b)/Math.log(2));
			return  a & (1<<shifts) - 1;
		}
		
		static int fastDivide(int a, int b) {
			// b is a power of 2
			int shifts = (int) (Math.log(b)/Math.log(2));
			return  a >> shifts;
		}
		
		static byte[] genPRBits(SecureRandom rnd, int len) {
			byte[] b = new byte[len];
			rnd.nextBytes(b);
			return b;
		}
	}

	protected void recursiveSave(ObjectOutputStream os) throws IOException
	{
		os.writeInt(dataSize);
		os.writeInt(extDataSize);
		//os.writeInt(keyLen); os.writeInt(nonceLen);
		os.writeInt(Z); os.writeInt(C);
		os.writeInt(stashSize);
		
		serverTree.save(os);
		
		os.writeInt(recLevel);
		
		os.write(clientKey);		
	}
	
	protected void initializeLoad(ExternalStorageInterface si, ObjectInputStream is) throws IOException
	{
		dataSize = is.readInt();
		extDataSize = is.readInt();
		
		//keyLen = is.readInt(); nonceLen = is.readInt();
		Z = is.readInt(); C = is.readInt();
		stashSize = is.readInt();
		
		serverTree = new Tree(si, is);
		
		
		
		recLevel = is.readInt();
		clientKey = new byte[keyLen];
		is.readFully(clientKey);
	}

	protected void recursiveLoad(ExternalStorageInterface esi, ObjectInputStream is, int i) throws IOException 
	{
		initializeLoad(esi, is);		
	}

	public int getRecursionLevels() { return 0; }
}