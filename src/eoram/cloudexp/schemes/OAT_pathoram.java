package eoram.cloudexp.schemes;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.data.*;
import eoram.cloudexp.implementation.*;
import eoram.cloudexp.interfaces.*;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.service.*;
import eoram.cloudexp.service.Request.RequestType;
import eoram.cloudexp.utils.Errors;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;

import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

/**
 * Implements basic PathORAM logic (see Stefanov, Emil, et al. "Path oram: An extremely simple oblivious ram protocol." ACM CCS 2013.).
 * <p><p>
 * This implementation is based on Java code obtained from authors of a follow-up work.
 * <p>
 *
 */
public class OAT_pathoram
{
	protected Log log = Log.getInstance();
	
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
	//================================================================================================================
	//=================================================== TREE CLASS =================================================
	//================================================================================================================
	class Tree {
		public int N; // the number of logic blocks in the tree
		public int D; // depth of the tree
//		int dataLen; // data length in bits
		
		ExternalStorageInterface storedTree;
		long treeSize;
		public Stash stash;

		Tree() { }
		////////!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		/*
		 * Given input "data" (to be outsourced), initialize the server side storage and return the client side position map. 
		 * No recursion on the tree is considered. 
		 */
		private int[] initialize(ExternalStorageInterface si, int maxBlocks, int dSize, BitSet[] data) 
		{
			storedTree = si;
			dataSize = dSize;
			extDataSize = dataSize + 4 + 4 + 4 + 4 + 4 + 4; //data + id + label + leftid + rightid + leftLabel + rightlabel
			
			// generate a random permutation for init, so we don't get a silly stash overflow
			List<Integer> permutation = new ArrayList<Integer>();
            for (int i = 0; i < maxBlocks; i++) { 
            	permutation.add(i); }
            Collections.shuffle(permutation);
            
			buildTree(maxBlocks, permutation, data);
			stash = new Stash(stashSize, recLevel, stashUseLS);
			
			
            // setup the position map according to the permuted blocks
 			//BitSet[] posMap = new BitSet[(N + C-1) / C];	// clientPosMap[i] is the leaf label of the i-th block.
 			//for (int i = 0; i < posMap.length; i++)	{ posMap[i] = new BitSet(C*D); }
 			
 			for (int i = 0; i < N; i++) 
 			{
 				int p = i;
 				if(i < permutation.size()) { p = permutation.get(i); }
 				//Utils.writePositionMap(posMap, this, i, p); 
 				System.out.println("[PO (initialize)] Block " + i + " -> leaf " + p);
 				//{ log.append("[POB (initialize)] Block " + i + " -> leaf " + p, Log.TRACE); }
 			}
 			int[] posMap = new int[1];
 			posMap[0] = permutation.get((maxBlocks-1)/2);
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
			final int logIntervalSize = 512;
			Vector<Pollable> v = new Vector<Pollable>();

			// initialize the tree
			treeSize = 2*N-1;
			int count = 0;
			for (int i = 0; i < treeSize; i++) 
			{
				Bucket temp;
				if (i < treeSize/2) { 	// DUMMY NODES => 2047-1024 = 1023
					temp = new Bucket(new Block()); } 
				else { 					// LEAF NODES
					if (i-N+1 < maxBlocks) // NON-DUMMY LEAF NODES
					{
						int id = permutation.indexOf(i-N+1); // make sure we are consistent with the permutation
						int label = i-N+1;
						int treeIndex;
						int leftId;
						int rightId;
						int leftLabel;
						int rightLabel;
						count++;
						ArrayList<Integer> tree = Utils.binaryTree(D-1);
						
						if (id % 2 == 1) {
							treeIndex = tree.indexOf(id);
							leftId = tree.get(2*treeIndex+1);
							rightId = tree.get(2*treeIndex+2);
							leftLabel = permutation.get(leftId);
							rightLabel = permutation.get(rightId);
						} 
						else {
							leftId = -1;
							rightId = -1;
							leftLabel = -1;
							rightLabel = -1;
						}
						
						int level = (int)(Math.log(tree.indexOf(id)+1)/Math.log(2));
						
						System.out.println(count + "Current ID: " + id + " at Level " + level + " leftID: " + leftId + " rightID: " + rightId + " leftLabel: " + leftLabel + " rightLabel: " + rightLabel);
						
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
							// DATA FROM DATAARRAY => BitSet
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
						temp = new Bucket(new Block(data, id, label, leftId, rightId, leftLabel, rightLabel));
						
						//{ log.append("[PathORAMBasic (BuildTree)] (R" + recLevel +") putting block " + id + " to label " + label + " (objectKey: " + recLevel + "#" + (i) + ").", Log.TRACE); }
					}
					else
						temp = new Bucket(new Block());
				}
				
				temp.encryptBlocks(); // ENCRYPT NEW TEMP BUCKET
				// CREATE NODES AS LOCAL FILES
				String objectKey = recLevel + "#" + (i);
				DataItem di = new SimpleDataItem(temp.toByteArray()); // CONVERT BUCKET TO DATA
				UploadOperation upload = new UploadOperation(Request.initReqId, objectKey, di);
				ScheduledOperation sop = storedTree.uploadObject(upload);
				///////////////////////////////////////////
				v.add(sop);
				
				if(i > 0 && (i % removeIntervalSize) == 0) 
				{
					Pollable.removeCompleted(v);
					
					if(v.size() >= (int)(removeIntervalSize * sizeFactorForSlowdown))
					{
						{ log.append("[PathORAMPointer (BuildTree)] Slowing down so storage can catch up...", Log.TRACE); }
						System.out.println("[PathORAMPointer (BuildTree)] Slowing down so storage can catch up...");
						int factor = (int)Math.ceil(v.size() / removeIntervalSize); if(factor > 5) { factor = 5; }
						try { Thread.sleep(factor * 5); } catch (InterruptedException e) { Errors.error(e); }
						Pollable.removeCompleted(v);
					}
				}
				
				if(i > 0 && (i % logIntervalSize) == 0)
				{
					Log.getInstance().append("[PathORAMPointer (BuildTree)] Created " + (i - v.size()) + " nodes of the Tree so far.", Log.TRACE);
					System.out.println("[PathORAMPointer (BuildTree)] Created " + (i - v.size()) + " nodes of the Tree so far.");
				}
			}
			
			// This waitForCompletion ensures can be used asynchronously!
			Pollable.waitForCompletion(v);
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

			int temp = leaf; //((leaf+1)>>1)-1;
			for (int i = 0; i < ret.length; i++) 
			{
				String objectKey = recLevel + "#" + (temp);
				DownloadOperation download = new DownloadOperation(reqId, objectKey);
				ScheduledOperation sop = storedTree.downloadObject(download);
				v.add(sop);
				
				// debug only
				//{ log.append("[POB (getBucketsFromPath)] reading down to leaf " + leaf + " (" + (leaf - (N-1))  + ") objectKey: " + objectKey, Log.ERROR); }
				System.out.println("[PO (getBucketsFromPath)] reading down to leaf " + leaf + " (" + (leaf - (N-1))  + ") objectKey: " + objectKey);
				
				if (temp > 0) { temp = ((temp+1)>>1)-1; }
			}
			
			Pollable.waitForCompletion(v);
			for (int i = 0; i < ret.length; i++) { ret[i] = new Bucket(v.get(i).getDataItem().getData()); }
			
			return ret;
		}
		//================================================================================================================
		// ================================================ BLOCK CLASS ==================================================
		//================================================================================================================
		class Block { 
			BitSet data;
			int id; // range: 0...N-1; // DUMMY BLOCKS ARE 'N'
			int treeLabel;
			int rightId;
			int leftId;
			int leftLabel;
			int rightLabel;
			
			private byte[] r; 
			
			public Block(Block blk) {
				assert (blk.data != null) : "no BitSet data pointers is allowed to be null.";
				try { data = (BitSet) blk.data.clone(); } 
				catch (Exception e) { e.printStackTrace(); System.exit(1); }
				id = blk.id;
				treeLabel = blk.treeLabel;
				r = blk.r;
				rightId = blk.rightId;
				leftId = blk.leftId;
				rightLabel = blk.rightLabel;
				leftLabel = blk.leftLabel;
				
			}
			
			Block(BitSet data, int id, int label, int leftId, int rightId, int leftLabel, int rightLabel) {
				assert (data != null) : "Null BitSet data pointer.";
				this.data = data;
				this.id = id;
				this.treeLabel = label;
				this.rightId = rightId;
				this.leftId = leftId;
				this.rightLabel = rightLabel;
				this.leftLabel = leftLabel;
			}
			
			public Block() {
				data = new BitSet(dataSize*8);
				id = N; // id == N marks a dummy block, so the range of id is from 0 to N, both ends inclusive. Hence the bit length of id is D+1.
				treeLabel = 0;
				leftId = -1;
				rightId = -1;
				leftLabel = -1;
				rightLabel = -1;
			}
			
			public Block(byte[] bytes) {
				byte[] bs = new byte[dataSize];
				ByteBuffer bb = ByteBuffer.wrap(bytes); //.order(ByteOrder.LITTLE_ENDIAN);
				bb = bb.get(bs);
				data = BitSet.valueOf(bs);
				id = bb.getInt();
				treeLabel = bb.getInt();
				leftId = bb.getInt();
				rightId = bb.getInt();
				leftLabel = bb.getInt();
				rightLabel = bb.getInt();
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
				leftId = bb.getInt();
				rightId = bb.getInt();
				leftLabel = bb.getInt();
				rightLabel = bb.getInt();
			}
			
			public boolean isDummy() {
				assert (r == null) : "isDummy() was called on encrypted block";
				return id == N;
			}
			
			public void erase() { id = N; treeLabel = 0; leftId = -1; rightId = -1; leftLabel = -1; rightLabel = -1; }
			
			
			public byte[] toByteArray() {
				ByteBuffer bb = ByteBuffer.allocate(extDataSize+nonceLen);

				// convert data into a byte array of length 'dataSize'
				byte[] d = new byte[dataSize];
				byte[] temp = data.toByteArray();
				for (int i=0; i<temp.length; i++) {
					d[i] = temp[i];
			    }
				
				bb.put(d);
				
				bb.putInt(id).putInt(treeLabel).putInt(leftId).putInt(rightId).putInt(leftLabel).putInt(rightLabel);
						
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
					leftId ^= ByteBuffer.wrap(mask, dataSize+4+4, 4).getInt();
					rightId ^= ByteBuffer.wrap(mask, dataSize+4+4+4, 4).getInt();
					leftLabel ^= ByteBuffer.wrap(mask, dataSize+4+4+4, 4).getInt();
					rightLabel ^= ByteBuffer.wrap(mask, dataSize+4+4+4+4, 4).getInt();
					
				} catch (Exception e) { 
					e.printStackTrace(); 
					System.exit(1); 
				}
			}
			
			@Override
			public boolean equals(Object blk)
			{
			    boolean isEqual= false;

			    if (blk != null && blk instanceof Block)
			    {
			        isEqual = (this.id == ((Block) blk).id);
			    }

			    return isEqual;
			}

			@Override
			public int hashCode() {
			    return this.id;
			}
		}
		//================================================================================================================
		//============================================== BUCKET CLASS ====================================================
		//================================================================================================================
		class Bucket { 
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
		//================================================================================================================
		//=================================================== STASH CLASS ================================================
		//================================================================================================================
		class Stash
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
			
			stash = new Stash(is);
		}
	}
	
	public OAT_pathoram (SecureRandom rand) 
	{
		rnd = rand;
		clientKey = Utils.genPRBits(rnd, keyLen);
	}
	
	/*
	 * Each data has 'unitB' bytes.
	 * 'data' will be intact since the real block has a clone of each element in 'data'.
	 */
	
	//============= PATH ORAM INITIALIZE ================================================================
	public int[] initialize(ExternalStorageInterface si, int maxBlocks, int unitB, BitSet[] data, int recLevel) {
		assert(maxBlocks < (~(1<<63))) : "Too many blocks in a tree.";
		
		// TODO => serverTree is An Array of Trees now (For loop acc. to maxBlocks)
		this.recLevel = recLevel;
		int nextB = unitB;
		serverTree = new Tree();////////////////////SERVER TREE
		int[] posMap = serverTree.initialize(si, maxBlocks, nextB, data);////////////////////POSITION MAP

		return posMap;
	}

	public enum OpType {Read, Write};
	protected Tree.Block access(long reqId, int[] posMap, OpType op, int a, BitSet data)
	{
		// TODO => Now it is a array
		Tree tr = serverTree;
		//                                            (PositionMap, Tree, Key)
		//int leafLabel = tr.N-1 + Utils.readPositionMap(posMap, tr, a);
		//Tree.Block[] blocks = tr.readBuckets(reqId, leafLabel);
		int leafLabel = posMap[0] + tr.N-1;
		int newLabel = rnd.nextInt(tr.N);
		posMap[0] = newLabel;
		//Utils.writePositionMap(posMap, tr, a, newLabel); //(u % s.C)*s.D, newlabel, s.D);
		
		// debug only
		//{ log.append("[POB (access)] (R" + recLevel + ") Block with id " + a + " should be in the path down to leaf " + leafLabel + " (label: " + (leafLabel - (tr.N-1)) + ") or in the stash (new label: " + newLabel + ").", Log.TRACE); }
		System.out.println("[PO (access)] Search for ID " + a + " would start from the root in the Path down to leaf " + leafLabel + " (label: " + (leafLabel - (tr.N-1)) + ") or in the Stash (new label: " + newLabel + ").");
		
		return rearrangeBlocksAndReturn(reqId, op, a, data, leafLabel, newLabel);
	}

	protected Tree.Block rearrangeBlocksAndReturn(long reqID, OpType op, int a, BitSet data, int leafLabel, int newlabel)
	{
		Tree tr = serverTree;
		Tree.Block res = null;
		Tree.Block[] blocks = tr.readBuckets(reqID, leafLabel);
		//Tree.Block[] union = new Tree.Block[tr.stash.size+blocks.length*(tr.D)];
		ArrayList<Tree.Block> unionList = new  ArrayList<Tree.Block>();
		
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
			for (int i = 0; i < tr.stash.size; i++) { 
				//union[i] = tr.new Block(v.get(i).getDataItem().getData(), true);
				unionList.add(tr.new Block(v.get(i).getDataItem().getData(), true));
			}
		}
		else
		{
			// ======== OLD STASH IS ADDED TO UNION ================
			for (int i = 0; i < tr.stash.size; i++)
			{
				Tree.Block blk = null;
				if(i < tr.stash.blocks.size()){
					blk = tr.stash.blocks.get(i);
					if (!unionList.contains(blk))
						unionList.add(blk);
				}
				else {
					blk = tr.new Block();
					unionList.add(blk);
				}
					 
				//union[i] = blk;
				
			}
		}
		
		// ======== NEW BLOCK ARE ADDED TO UNION ============
		for (Tree.Block blk : blocks) { 
			//union[i+tr.stash.size] = tr.new Block(blocks[i]); 
			blk.dec(); 
			if (!unionList.contains(blk))
				unionList.add(blk);
		}
		
		// ======== NEW BLOCKs ARE DECRYPTED ==============
//		for (Tree.Block blk : unionList)
//		{
//			if (blk.r != null) // when r == null, the block comes from the stash, already decrypted.
//			{ blk.dec(); }
//		}
		//======== FIRST LOOK AT ROOT THEN DOWNLOAD ACC. TO CHILDREN ====================================
		int current = (tr.N-2)/2;
		int[] retrieved = new int[tr.D];
		
		for (int k = 0; current != -1 ; k++){ //k < tr.D-1
			Tree.Block blk = null;
			boolean match = false;
			for (int i = 0; i < unionList.size() && !match; i++) 
			{	
				if (unionList.get(i).id == current) {
					retrieved[k] = i;
					blk = unionList.get(i);	
					match = true;
				}
			}
			
			if (match){
				System.out.println("SUCCESS: Block is retrieved " + blk.id);
				System.out.println("Current Level " + k + " & Retrieved ID " + blk.id + " & Childrens at " + blk.leftLabel + " & " + blk.rightLabel);
			}
			else {System.out.println("ERROR: Block cannot be retrieved"); }
			
			if (blk.id == a) {
				res = tr.new Block(blk);
				if (op == OpType.Write) {blk.data = data;}
				current = blk.rightId;
				if (current != -1){
					blocks = tr.readBuckets(reqID, blk.rightLabel + tr.N-1);
					for (Tree.Block blks : blocks) { 
						blks.dec(); 
						if (!unionList.contains(blks))
							unionList.add(blks);
					}
				}
			}
			else if (a < blk.id){
				current = blk.leftId;
				if (current != -1){
					blocks = tr.readBuckets(reqID, blk.leftLabel + tr.N-1);
					for (Tree.Block blks : blocks) { 
						blks.dec(); 
						if (!unionList.contains(blks))
							unionList.add(blks);
					}
				}
				
			}
			else {
				current = blk.rightId;
				if (current != -1){
					blocks = tr.readBuckets(reqID, blk.rightLabel + tr.N-1);
					for (Tree.Block blks : blocks) { 
						blks.dec(); 
						if (!unionList.contains(blks))
							unionList.add(blks);
					}
				}
			}
		}
		//======== CHILDREN LABELS ARE UPDATED IN REVERSE ORDER ====================================
		int [] oldLabels = new int [retrieved.length];
		for (int k = retrieved.length - 1; k >= 0; k--){
			if (k == 0){
				oldLabels[k] = unionList.get(retrieved[k]).treeLabel + tr.N-1;
				unionList.get(retrieved[k]).treeLabel = newlabel;	
				System.out.println("ID " + unionList.get(retrieved[k]).id + " was in " + oldLabels[k] + " and will be put in Leaf " + unionList.get(retrieved[k]).treeLabel + ". Children would be in " + unionList.get(retrieved[k]).leftLabel + " & " + unionList.get(retrieved[k]).rightLabel);
			}
				
			else {
				oldLabels[k] = unionList.get(retrieved[k]).treeLabel + tr.N-1;
				unionList.get(retrieved[k]).treeLabel = rnd.nextInt(tr.N);				
				if(unionList.get(retrieved[k]).id > unionList.get(retrieved[k-1]).id){
					unionList.get(retrieved[k-1]).rightLabel = unionList.get(retrieved[k]).treeLabel;
					System.out.println("ID " + unionList.get(retrieved[k]).id + " was in " + oldLabels[k] + " and will be put in Leaf " + unionList.get(retrieved[k]).treeLabel + ". Children would be in " + unionList.get(retrieved[k]).leftLabel + " & " + unionList.get(retrieved[k]).rightLabel);
				}
				else{
					unionList.get(retrieved[k-1]).leftLabel = unionList.get(retrieved[k]).treeLabel;
					System.out.println("ID " + unionList.get(retrieved[k]).id + " was in " + oldLabels[k] + " and will be put in Leaf " + unionList.get(retrieved[k]).treeLabel + ". Children would be in " + unionList.get(retrieved[k]).leftLabel + " & " + unionList.get(retrieved[k]).rightLabel);
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
			Errors.error("[PO (rearrangeBlocksAndReturn)] Couldn't find block with id " + a + " in the path down to label " + (leafLabel - (tr.N-1))+ " or in the stash.");
			System.out.println(" ");
			System.out.println("[PO (rearrangeBlocksAndReturn)] Couldn't find block with id " + a + " in the path down to label " + (leafLabel - (tr.N-1))+ " or in the stash.");
			System.out.println(" ");
		}
		
		//List<Tree.Block> unionList = Arrays.asList(union);
		Collections.shuffle(unionList, rnd);
		//union = unionList.toArray(new Tree.Block[0]);
		
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
		
		//======== EACH DOWNLOADED (logN) PATH  IS EVICTED BY INCLUDING MATCHING BLOCKS FROM STASH ====================================
		v.clear();
		ArrayList<String> uploaded = new ArrayList<String>();
		for (int m = retrieved.length - 1; m >= 0; m--){
			int label = oldLabels[m];
			for (int i = tr.D+1; i > 0; i--) 
			{
				int prefix = Utils.iBitsPrefix(label+1, tr.D+1, i);
				
				String objectKey = recLevel + "#" + ((prefix>>(tr.D+1-i))-1);
				
				if(!uploaded.contains(objectKey)){
					Tree.Bucket bucket = tr.new Bucket(tr.new Block());
					for (int j = 0, k = 0; j < unionList.size() && k < Z; j++) 
					{
						if (!unionList.get(j).isDummy())
						{ 
							int jprefix = Utils.iBitsPrefix(unionList.get(j).treeLabel+tr.N, tr.D+1, i);
							
							if(prefix == jprefix) 
							{
								bucket.blocks[k++] = tr.new Block(unionList.get(j));
								
								// debug only
								//{ log.append("[POB (rearrangeBlocksAndReturn)] Block with id " + union[j].id + " will be re-written in bucket on the path to label " + union[j].treeLabel + " (objectKey: " + objectKey + ")", Log.TRACE); }
								System.out.println("[PO (rearrangeBlocksAndReturn)] Block with id " + unionList.get(j).id + " will be re-written in bucket on the path of Leaf-" + unionList.get(j).treeLabel + " (objectKey: " + objectKey + ")");
								unionList.get(j).erase();
							}
						}
					}
					
					
					/*for(int k=0; k<Z; k++) // debug only
					{
						Tree.Block blk = bucket.blocks[k];
						if(blk.isDummy() == false)
						{
							{ log.append("[PO (rearrangeBlocksAndReturn)] Bucket " + objectKey + " contains block with id " + blk.id + " with label " + blk.treeLabel, Log.TRACE); }
						}
					}*/
				
				
				
					bucket.encryptBlocks();		
					
					DataItem di = new SimpleDataItem(bucket.toByteArray());
					UploadOperation upload = new UploadOperation(reqID, objectKey, di);
					ScheduledOperation sop = tr.storedTree.uploadObject(upload);
					uploaded.add(objectKey);
					
					v.add(sop);
				}
				else {
					//System.out.println(objectKey + " is already uploaded");
				}
				
			}
		}
		
		Pollable.waitForCompletion(v); v.clear(); // wait and clear
		
		if(tr.stash.ls == null) { tr.stash.blocks.clear(); } // clear stash in memory
		
		// put the rest of the blocks in 'union' into the 'stash'
		int j = 0, k = 0;
		for (; j < unionList.size() && k < tr.stash.size; j++) 
		{
			if (unionList.get(j).isDummy() == false) 
			{
				if(tr.stash.ls != null) // use LS
				{
					String objectKey = recLevel + "#" + (k);
					DataItem di = new SimpleDataItem(unionList.get(j).toByteArray());
					UploadOperation upload = new UploadOperation(reqID, objectKey, di);
					ScheduledOperation sop = tr.stash.ls.uploadObject(upload);
					sop.waitUntilReady();
				}
				else
				{
					tr.stash.blocks.add(tr.new Block(unionList.get(j)));
					System.out.println(unionList.get(j).id + " could not be evicted, stayed in Stash");
				}
				
				k++;
				unionList.get(j).erase();
			}
		}
		
		System.out.println("CURRENT STASH SIZE = " + k);
		
		if (k == tr.stash.size) 
		{
			for (; j < unionList.size(); j++)
			{ 
				assert (unionList.get(j).isDummy()) : "Stash is overflown: " + tr.stash.size; 
			}	
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
	//========================= TREE READ ===================================================================
	Tree.Block read(long reqID, int[] pm, int i) { return access(reqID, pm, OpType.Read, i, null); }
	//========================= TREE WRITE ==================================================================
	Tree.Block write(long reqID, int[] pm, int i, BitSet d) { return access(reqID, pm, OpType.Write, i, d); }

	
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

//			System.out.println(Integer.toString(val) + " " + Integer.toBinaryString(val) + " " + map.toString());
			
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
		
		static ArrayList<Integer> binaryTree(int h){
			ArrayList<Integer> result = new ArrayList<Integer>((int) (Math.pow(2, h)-1));
			int root = (int)(Math.pow(2, h)-1);
			result.add(root);
			int k = 0;
			for (int i = 1; i <= h; i++){
				for (int j = 0; j < Math.pow(2,i); j++){
					int node;
					if (j%2 == 0){
						node = (int) (result.get(k/2) - Math.pow(2, h-i));
						result.add(node);
						k++;
					}
					else {
						node = (int) (result.get((k-1)/2) + Math.pow(2, h-i));
						result.add(node);
						k++;
					}
				}
			}	
			return result;
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