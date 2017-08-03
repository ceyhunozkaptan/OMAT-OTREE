package eoram.cloudexp.schemes;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.data.*;
import eoram.cloudexp.implementation.*;
import eoram.cloudexp.interfaces.*;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.schemes.ObliviousMatrix.Matrix.Block;
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
 * 
 * 
 *
 */
public class ObliviousMatrix
{
	protected Log log = Log.getInstance();
	
	SessionState ss = SessionState.getInstance();
	ClientParameters clientParams = ClientParameters.getInstance();
	
	final File matFile = new File("./log/" + ss.client + "_" + ss.storage + "_" + clientParams.maxBlocks + "_" + ss.experimentHash + ".perfeval2.txt");
	final File matFile2 = new File("./log/" + "matrix_experiment_homesetting.txt");
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
//	public static int NoOfColLeafs = 8;
	public static int stashSize = 89; // set according to table 3 of PathORAM paper (for 80-bit security)
	
	public static int C = 1;
	//public static int C = 4; // memory reduction factor (ensure it is a power of 2)
	
	public static final boolean stashUseLS = false;
	
	Matrix serverMatrix;
	int recLevel;
		
	byte[] clientKey;
	//================================================================================================================
	//=================================================== MATRIX CLASS =================================================
	//================================================================================================================
	class Matrix {
		public int Nrow; // the number of logic blocks in the tree
		public int Ncol;
		
		public int Drow; // depth of the tree
		public int Dcol;
		
		public int row;
		public int col;
		
		ExternalStorageInterface storedMatrix;
		long matrixSize;
		public Stash stash;

		Matrix() { }
		////////!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		/*
		 * Given input "data" (to be outsourced), initialize the server side storage and return the client side position map. 
		 * No recursion on the tree is considered. 
		 */
		private Object[] initialize(ExternalStorageInterface si, int maxBlocks, int dSize, BitSet[][] data) 
		{
			storedMatrix = si;
			dataSize = dSize;
			extDataSize = dataSize + 4 + 4 + 4 + 4; //data + rowid + colid + rowlabel + collabel
			
//			Nrow = (int) Math.pow(2, Math.ceil(Math.log(maxBlocks)/Math.log(2)))/2; // CEIL MAX BLOCK TO 2^x
			Nrow = 16384;//512; 16384;

			Drow = Utils.bitLength(Nrow)-1;
			
			matrixSize = 2*Nrow-1;
			row = 2*Nrow-1;
			
			Ncol = (int) Math.pow(2, Math.ceil(Math.log(maxBlocks)/Math.log(2)))/2;
			
//			col = 2*N-1;
//			Ncol = 4;
			Dcol = Utils.bitLength(Ncol)-1;
			col = 2*Ncol-1;
			
			System.out.println("# of Blocks = " + maxBlocks + "");
			System.out.println("# of Rows = " + row + "");
			System.out.println("# of RowLeafs = " + Nrow + "");
			System.out.println("# of Cols = " + col + "");
			System.out.println("# of ColLeafs = " + Ncol + "");

			// generate a random permutation for init, so we don't get a silly stash overflow
			List<Integer> permutationRow = new ArrayList<Integer>();
			List<Integer> permutationCol = new ArrayList<Integer>();
			
			int leafStart = row/2;
            for (int i = 0; i < row; i++) { 
        		int leafNo = i;
            	while (leafNo < leafStart){
        			leafNo = ((leafNo)<<1)+1;
        		}        	
            	permutationRow.add(leafNo-Nrow+1);
        		System.out.println("[PO (initialize)] Row-" + i + " -> leaf " + permutationRow.get(i));  	
            }
            
            leafStart = col/2;
            for (int i = 0; i < col; i++) { 
        		int leafNo = i;
            	while (leafNo < leafStart){
        			leafNo = ((leafNo)<<1)+1;
        		}        	

        		permutationCol.add(leafNo-Ncol+1); 
        		System.out.println("[PO (initialize)] Column-" + i + " -> leaf " + permutationCol.get(i));  	
            }
            
			buildMatrix(maxBlocks, permutationRow, permutationCol, data);
			stash = new Stash(stashSize, recLevel, row, col,  stashUseLS);
			
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
			
			return new Object []{permutationRow, permutationCol};
		}
		
		private void buildMatrix(int maxBlocks, List<Integer> permutation1, List<Integer> permutation2,  BitSet[][] dataArray) 
		{
			
			SessionState ss = SessionState.getInstance();
			Map<String, Request> fastInitMap = ss.fastInitMap;//////////// FAST INIT MAP !!!!!!!!!!!!!!!!!!!!!!!!!
			if(ss.fastInit == false) {  fastInitMap = null; }
			
			// set N to be the smallest power of 2 that is bigger than 'data.length'. 
			final int removeIntervalSize = 512; final double sizeFactorForSlowdown = 0.75;
			final int logIntervalSize = 2048;
			Vector<Pollable> v = new Vector<Pollable>();
			
			
			ArrayList<UploadOperation> uploadList = new ArrayList<UploadOperation>();
			// initialize the matrix structure based on PathORAM scheme		
			
			BitSet data = null;
			int rowId; // make sure we are consistent with the permutation
			int colId;
			int rowLabel;
			int colLabel;
			Bucket temp;
			DataItem di = null;
			UploadOperation upload = null;
			ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
			for (int i = 0; i < row; i++) 
			{
				
				
				String objectKey = "";
				long start = System.nanoTime();
				
				for (int j = 0; j < col; j++)
				{
					rowId = i; // make sure we are consistent with the permutation
					colId = j;
					rowLabel = permutation1.get(i);
					colLabel = permutation2.get(j);
					
															
//					System.out.println("[ObliviousMatrix (BuildMatrix)] CurrentID: " + rowId + "#" + colId + "|| LeafLabel" + rowLabel + " & " + colLabel);
	
					String blockIdStr = "" + rowId;
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
//							Errors.verify(dataArray.length > rowId);
//							data = dataArray[rowId][colId];
//							data = dataArray[rowId][colId];
						}
						else
						{
							long[] val = new long[2]; 
							val[0] = rowId;
							val[1] = colId;
							data = BitSet.valueOf(val);
						}
					}
					temp = new Bucket(new Block(new BitSet(), rowId, colId, rowLabel, colLabel));
//					System.out.println("[ObliviousMatrix (BuildMatrix)] Putting block with" + rowId + "#" + colId + " to label " + rowLabel + "&" + colLabel + ".");
								
					temp.encryptBlocks(); // ENCRYPT NEW TEMP BUCKET
					// CREATE NODES AS LOCAL FILES
					
					objectKey = rowId + "#" + col;
					di = new SimpleDataItem(temp.toByteArray()); // CONVERT BUCKET TO DATA	
					upload = new UploadOperation(Request.initReqId, objectKey, di);
					uploadList.add(upload);
				}
				
//				ArrayList<ScheduledOperation> sop = storedMatrix.uploadRow(uploadList);
//
//				v.addAll(sop);
				
//				if(i > 0 && (i % removeIntervalSize) == 0) 
//				{
//					Pollable.removeCompleted(v);
//					
//					if(v.size() >= (int)(removeIntervalSize * sizeFactorForSlowdown))
//					{		
//						{ log.append("[ObliviousMatrix (BuildMatrix)] Slowing down so storage can catch up...", Log.TRACE); }
//						System.out.println("[ObliviousMatrix (BuildMatrix)] Slowing down so storage can catch up...");
//						int factor = (int)Math.ceil(v.size() / removeIntervalSize); if(factor > 5) { factor = 5; }
//						try { Thread.sleep(factor * 5); } catch (InterruptedException e) { Errors.error(e); }
//						Pollable.removeCompleted(v);
//					}
//				}
				
				long end = System.nanoTime();
				System.out.println("RowID: " + i + " is created!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! in " + (end - start) / 1000000.0 + " ms");
				
				if(i >= logIntervalSize && (i % logIntervalSize) == 0)
				{
					Log.getInstance().append("[ObliviousMatrix (BuildMatrix)] Created " + (i - v.size()) + " nodes of the Tree so far.", Log.TRACE);
					System.out.println("[ObliviousMatrix (BuildMatrix)] Created " + (i - v.size()) + " nodes of the Tree so far.");
					if(!uploadList.isEmpty()){
						sop = storedMatrix.uploadAllRow(uploadList);
						v.addAll(sop);
						uploadList.clear();
						sop = null;		
						Pollable.waitForCompletion(v);
						v.clear();
						System.gc();
					}				
				}
			}	
			
			if(!uploadList.isEmpty()){
				sop = storedMatrix.uploadAllRow(uploadList);
				v.addAll(sop);
				uploadList = null;
				sop = null;
				Pollable.waitForCompletion(v);
				System.gc();
			}
			
//			System.gc();
			
		}

		
		protected Block[][] readBuckets(long reqId, int leafLabel, int dim) {
			Bucket[][] buckets = getBucketsFromPath(reqId, leafLabel, dim);		
			Block[][] res = null;
			if (dim == 0){
				res = new Block[Z*(Drow+1)][Z*col];
				for (int i = 0; i < Drow+1 ;i++) 
				{
					for (int j = 0; j < col ;j++)
					{
						Block[][] blocks = buckets[i][j].blocks;		
						buckets[i][j] = null;
						for (int k = 0; k < Z; k++){
							for (int m = 0; m < Z; m++){
								res[Z*i+k][Z*j+m] = blocks[k][m];
							}
						}
					}
				}
			}
			else if (dim ==1)
			{
				res = new Block[Z*(Dcol+1)][Z*row];
				for (int i = 0; i < Dcol+1 ;i++) 
				{
					for (int j = 0; j < row ;j++)
					{
						Block[][] blocks = buckets[i][j].blocks;
						buckets[i][j] = null;
						for (int k = 0; k < Z; k++){
							for (int m = 0; m < Z; m++){
								res[Z*i+k][Z*j+m] = blocks[k][m];
							}
						}
					}
				}
			}
			buckets = null;
			return res;
		}

		private Bucket[][] getBucketsFromPath(long reqId, int leaf, int dim) 
		{
			Bucket[][] ret = null;
			
			Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
					
			if (dim == 0){ // ROW
				ret = new Bucket[Drow+1][col];
								
				long start = System.nanoTime();
				String objectKey = leaf + "#" + col;
				DownloadBulkOperation download = new DownloadBulkOperation(reqId, objectKey, dim);
				ScheduledOperation sop = storedMatrix.downloadRow(download);
				long end = System.nanoTime();
				v.add(sop);
				sop = null;
				
				System.out.println("[PO (getBucketsFromPath)] Read from Root to RowLeaf-(" + (leaf - (Nrow-1))  + ") objectKey: " + objectKey + " in " + (end - start) / 1000000.0 + " ms");

				long start2 = System.nanoTime();
				Pollable.waitForCompletion(v);
				long end2 = System.nanoTime();
				System.out.println("Waited For " + (end2 - start2) / 1000000.0 + " ms");
				
				ArrayList<DataItem> dataItemList = v.get(0).getDataItemList();
				v = null;
				
				for (int i = 0; i < Drow+1; i++) {
					for (int k = 0; k < col; k++){
						ret[i][k] = new Bucket(dataItemList.get(k+(i)*col).getData());
						dataItemList.set(k+(i)*col, null);
					}					
				}
				dataItemList = null;
			}
			else if (dim == 1){
				ret = new Bucket[Dcol+1][row];
				
				long start = System.nanoTime();
				String objectKey = leaf + "#" + row;
				DownloadBulkOperation download = new DownloadBulkOperation(reqId, objectKey, dim);
				ScheduledOperation sop = storedMatrix.downloadCol(download);
				long end = System.nanoTime();
				v.add(sop);
				sop = null;
					
				System.out.println("[PO (getBucketsFromPath)] Read from Root to ColLeaf-(" + (leaf - (Ncol-1))  + ") objectKey: " + objectKey + " in " + (end - start) / 1000000.0 + " ms");

				long start2 = System.nanoTime();
				Pollable.waitForCompletion(v);
				long end2 = System.nanoTime();
				System.out.println("Waited For " + (end2 - start2) / 1000000.0 + " ms");
				
				ArrayList<DataItem> dataItemList = v.get(0).getDataItemList();
				v = null;
				
				for (int i = 0; i < Dcol+1; i++) {
					for (int k = 0; k < row; k++){
						ret[i][k] = new Bucket(dataItemList.get(i+(k)*(Dcol+1)).getData());
						dataItemList.set(i+(k)*(Dcol+1), null);
					}					
				}
				dataItemList = null;
			}
			return ret;
		}
		//================================================================================================================
		// ================================================ BLOCK CLASS ==================================================
		//================================================================================================================
		class Block { 
			BitSet data;
			int rowId; // range: 0...N-1; // DUMMY BLOCKS ARE 'N'
			int colId;
			int rowLabel;
			int colLabel;

			private byte[] r = new byte[nonceLen];
			
			public Block(Block blk) {
				assert (blk.data != null) : "no BitSet data pointers is allowed to be null.";
				try { data = (BitSet) blk.data.clone(); } 
				catch (Exception e) { e.printStackTrace(); System.exit(1); }
				rowId = blk.rowId;
				colId = blk.colId;
				rowLabel = blk.rowLabel;
				colLabel = blk.colLabel;
				r = blk.r;

				
			}
			
			Block(BitSet data, int rowId,int colId, int rowLabel, int colLabel) {
				assert (data != null) : "Null BitSet data pointer.";
				this.data = data;
				this.rowId = rowId;
				this.colId = colId;
				this.rowLabel = rowLabel;
				this.colLabel = colLabel;
			}
			
			public Block() {
				data = new BitSet(dataSize*8);
				rowId = -1; // id == N marks a dummy block, so the range of id is from 0 to N, both ends inclusive. Hence the bit length of id is D+1.
				colId = -1;
				rowLabel = -1;
				colLabel = -1;
			}
			
			public Block(byte[] bytes) {
				byte[] bs = new byte[dataSize];
				ByteBuffer bb = ByteBuffer.wrap(bytes); //.order(ByteOrder.LITTLE_ENDIAN);
				bb = bb.get(bs);
				data = BitSet.valueOf(bs);
				rowId = bb.getInt();
				colId = bb.getInt();
				rowLabel = bb.getInt();
				colLabel = bb.getInt();
				r = new byte[nonceLen];
				bb.get(r);
			}
			
			public Block(byte[] bytes, boolean stash) {
				byte[] bs = new byte[dataSize];
				ByteBuffer bb = ByteBuffer.wrap(bytes); //.order(ByteOrder.LITTLE_ENDIAN);
				bb = bb.get(bs);
				data = BitSet.valueOf(bs);
				rowId = bb.getInt();
				colId = bb.getInt();
				rowLabel = bb.getInt();
				colLabel = bb.getInt();

			}
			
			public boolean isDummy() {
				assert (r == null) : "isDummy() was called on encrypted block";
				return rowId == -1;
			}
			
			public void erase() { rowId = -1; colId = -1; rowLabel = -1; colLabel = -1;  }
			
			
			public byte[] toByteArray() {
				ByteBuffer bb = ByteBuffer.allocate(extDataSize+nonceLen);

				// convert data into a byte array of length 'dataSize'
				byte[] d = new byte[dataSize];
				byte[] temp = data.toByteArray();
				for (int i=0; i<temp.length; i++) {
					d[i] = temp[i];
			    }
				
				bb.put(d);
				
				bb.putInt(rowId).putInt(colId).putInt(rowLabel).putInt(colLabel);
						
				bb.put((r == null) ? new byte[nonceLen] : r);
					
				return bb.array();
			}
			
			public String toString() {return Arrays.toString(toByteArray());}
			
			private void enc() {
				//FIXME
//				r = Utils.genPRBits(rnd, nonceLen);
				mask();
			}
			
			private void dec() { 
				mask();
				//FIXME
				//r = null;
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
					rowId ^= ByteBuffer.wrap(mask, dataSize, 4).getInt();
					colId ^= ByteBuffer.wrap(mask, dataSize+4, 4).getInt();
					rowLabel ^= ByteBuffer.wrap(mask, dataSize+4+4, 4).getInt();
					colLabel ^= ByteBuffer.wrap(mask, dataSize+4+4+4, 4).getInt();

					
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
			        isEqual = (this.rowId == ((Block) blk).rowId) && (this.colId == ((Block) blk).colId);
			    }

			    return isEqual;
			}

			@Override
			public int hashCode() {
			    return this.rowId;
			}
		}
		//================================================================================================================
		//============================================== BUCKET CLASS ====================================================
		//================================================================================================================
		class Bucket { 
			Block[][] blocks = new Block[Z][Z];
			
			Bucket(Block b) { // BUCKET WITH Block b
				assert (b != null) : "No null block pointers allowed.";
				for (int i = 0; i < Z; i++)
					for (int j = 0; j < Z; j++)// FILL OTHER BLOCK WITH DUMMY
						blocks[i][j] = new Block();
				blocks[0][0] = b;

			}
			
			Bucket(byte[] array) {
				ByteBuffer bb = ByteBuffer.wrap(array);
				byte[] temp = new byte[extDataSize+nonceLen];
				for (int i = 0; i < Z; i++) {
					for (int j = 0; j < Z; j++){
						bb.get(temp);
						blocks[i][j] = new Block(temp);
					}
				}
			}
			
			public byte[] toByteArray() {
				ByteBuffer bb = ByteBuffer.allocate(Z * Z * (extDataSize+nonceLen));
				for (int i = 0; i < Z; i++)
					for (int j = 0; j < Z; j++)//
						bb.put(blocks[i][j].toByteArray());
				return bb.array();
			}

			void encryptBlocks() { // ENCRYPT 1 BUCKET
				for (int i = 0; i < Z; i++)
					for (int j = 0; j < Z; j++)// FILL OTHER BLOCK WITH DUMMY
						blocks[i][j].enc();
			}
		}
		//================================================================================================================
		//=================================================== STASH CLASS ================================================
		//================================================================================================================
		class Stash
		{
			LocalStorage ls = null;
			ArrayList<ArrayList<Block>> colBlocks = null;
			ArrayList<ArrayList<Block>> rowBlocks = null;
			int size = 0; // in blocks
			
			public Stash(int size, int recLevel, int row, int col,  boolean useLS) 
			{
				this.ls = null;
				this.colBlocks = null;
				this.rowBlocks = null;
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
					colBlocks = new ArrayList<ArrayList<Block>>();
					rowBlocks = new ArrayList<ArrayList<Block>>();
					for (int i = 0; i < col; i++)
						colBlocks.add(new ArrayList<Block>());
					for (int i = 0; i < row; i++)
						rowBlocks.add(new ArrayList<Block>());
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
											
						os.writeInt(data.length);
						os.write(data);
					}
				}
				else
				{
					os.writeInt(colBlocks.size());
					for(int i=0; i<colBlocks.size(); i++)
					{
						for (int k = 0; k < colBlocks.get(i).size(); k++){
							Block blk = colBlocks.get(i).get(k);
							byte[] data = blk.toByteArray();
												
							os.writeInt(data.length);
							os.write(data);
						}					
					}
				}
			}
			//FIXME
			public Stash(ObjectInputStream is) throws IOException
			{
				this.ls = null;
				this.colBlocks = null;
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
						
						String objectKey = recLevel + "#" + (i);
						DataItem di = new SimpleDataItem(data);
						UploadOperation upload = new UploadOperation(Request.initReqId, objectKey, di);
						ScheduledOperation sop = ls.uploadObject(upload);
						sop.waitUntilReady();
					}
				}
				else
				{
					colBlocks = new ArrayList<ArrayList<Block>>();
					
					int s = is.readInt();
					for(int i=0; i < s; i++)
					{
						int byteSize = is.readInt();
						byte[] data = new byte[byteSize];
						is.readFully(data);
						
//						Block blk = new Block(data, true);
						Block blk = new Block();
												
					}
				}
			}
		}

		protected void save(ObjectOutputStream os) throws IOException
		{
			os.writeInt(Nrow);
			os.writeInt(Drow);
			
			os.writeInt(Ncol);
			os.writeInt(Dcol);

			os.writeLong(matrixSize);
			
			stash.save(os);
		}
		
		public Matrix(ExternalStorageInterface si, ObjectInputStream is)  throws IOException
		{
			storedMatrix = si;
			Nrow = is.readInt();
			Drow = is.readInt();
			
			Ncol = is.readInt();
			Dcol = is.readInt();
			
			matrixSize = is.readLong();
			
			row = 2*Nrow-1;
			col = 2*Ncol-1;
			
			
			stash = new Stash(stashSize, recLevel, row, col,  stashUseLS);
			
			
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
			
			
//			stash = new Stash(is);
		}
	}
	
	public ObliviousMatrix (SecureRandom rand) 
	{
		rnd = rand;
		clientKey = Utils.genPRBits(rnd, keyLen);
	}
	
	/*
	 * Each data has 'unitB' bytes.
	 * 'data' will be intact since the real block has a clone of each element in 'data'.
	 */
	
	//============= PATH ORAM INITIALIZE ================================================================
	public Object[] initialize(ExternalStorageInterface si, int maxBlocks, int unitB, BitSet[][] data, int recLevel) {
		assert(maxBlocks < (~(1<<63))) : "Too many blocks in a tree.";
		
		this.recLevel = recLevel;
		int nextB = unitB;
		serverMatrix = new Matrix();

		Object[] posMap = serverMatrix.initialize(si, maxBlocks, nextB, data);////////////////////POSITION MAP

		return posMap;
	}

	public enum OpType {Read, Write};
	protected ArrayList<Matrix.Block> access(long reqId, Object[] posMap, OpType op, int a, BitSet data, int dim)
	{
		Matrix tr = serverMatrix;
		
		int leafLabel = 0;
		int newLabel = 0;
		
		if (dim == 0){
			newLabel = rnd.nextInt(tr.Nrow);
			leafLabel = ((ArrayList<Integer>) posMap[0]).get(a) + tr.Nrow-1;
			((ArrayList<Integer>) posMap[0]).set(a,newLabel);
			System.out.println("[PO (access to Row)] Search for rowID-" + a + " would start from the root in the Path down to leaf-" + leafLabel + " (label: " + (leafLabel - (tr.Nrow-1)) + ") or in the Stash (new label: " + newLabel + ").");
		}
		else if (dim == 1){
			newLabel = rnd.nextInt(tr.Ncol);
			leafLabel = ((ArrayList<Integer>) posMap[1]).get(a) + tr.Ncol-1;
			((ArrayList<Integer>) posMap[1]).set(a,newLabel);
			System.out.println("[PO (access to Col)] Search for ID " + a + " would start from the root in the Path down to leaf " + leafLabel + " (label: " + (leafLabel - (tr.Nrow-1)) + ") or in the Stash (new label: " + newLabel + ").");
		}
				
		// debug only
		//{ log.append("[POB (access)] (R" + recLevel + ") Block with id " + a + " should be in the path down to leaf " + leafLabel + " (label: " + (leafLabel - (tr.N-1)) + ") or in the stash (new label: " + newLabel + ").", Log.TRACE); }
		
		return rearrangeBlocksAndReturn(reqId, op, a, data, leafLabel, newLabel, dim);
	}

	protected ArrayList<Matrix.Block> rearrangeBlocksAndReturn(long reqID, OpType op, int a, BitSet data, int leafLabel, int newlabel, int dim)
	{
		Matrix matrix = serverMatrix;
		ArrayList<Matrix.Block> res = new ArrayList<Matrix.Block>();
		
		long totalTime = 0;
		
		try{
			fw = new FileWriter(matFile, true);		
			fw2 = new FileWriter(matFile2, true);	
		}
		catch(Exception e) { Errors.error(e); }
		bw = new BufferedWriter(fw);
		bw2 = new BufferedWriter(fw2);
		
		long start = System.nanoTime();
		Matrix.Block[][] blocks = matrix.readBuckets(reqID, leafLabel, dim); //ROOT BUCKET ONLY
		long end = System.nanoTime();
		try {
			bw.write((int) ((end - start) / 1000000.0)+", " );
			totalTime = (end - start);
		} catch (Exception e) { Errors.error(e); }
		System.out.println("==============================================================================");
		System.out.println("ALL DOWNLOAD TOOK " + (end - start) / 1000000.0 + " ms");
		System.out.println("==============================================================================");
		System.out.println("");
		System.out.println("DOWNLOAD IS DONE. \nBLOCKS WILL BE DECRYPTED & SEARCHED. \nNEW LABELS WILL BE GIVEN");
		System.out.println("");
		System.out.println("==============================================================================");
		
		ArrayList<ArrayList<Matrix.Block>>retrieved = new ArrayList<ArrayList<Matrix.Block>>();
		
		Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
				
		//======== FINDING THE ACCESSED BLOCK ====================================	
		long startEvict = 0;
		if (dim == 0){
			long startFind = System.nanoTime();
			// ======== OLD BLOCKS ARE ADDED FROM STASH ============		
			for (int i = 0; i < matrix.col ; i++){
				retrieved.add(matrix.stash.colBlocks.get(i));
				matrix.stash.colBlocks.get(i).clear();
			}
			//CHECK ALSO ROW STASH FOR BLOCKS	
			List<Integer> path = new ArrayList<Integer>();
			int temp = leafLabel;
			while (temp >= 0){
				path.add(temp);
				temp = ((temp+1)>>1)-1;
			}		
			for(int i = 0; i < path.size(); i++){
				ArrayList<Matrix.Block> rowStash = matrix.stash.rowBlocks.get(path.get(i));
				for (int k = 0; k < rowStash.size(); k++){
					Matrix.Block blk = rowStash.get(k);				
					if (!blk.isDummy()){
						if (blk.rowId == a) {
							res.add(blk);
							blk.rowLabel = newlabel;
							if (op == OpType.Write) {
								blk.data = data;
							}
						}
					}
				}
			}			
			// ======== NEW BLOCKS ARE ADDED ============
			for (int i = 0; i < Z*(matrix.Drow+1) ; i++) {
				for (int k = 0; k < Z*matrix.col ; k++){
					Matrix.Block blk = blocks[i][k];
					blk.dec(); 				
					if (!blk.isDummy()){
						if (blk.rowId == a) {
							res.add(blk);
//							System.out.println("SUCCESS: Block is retrieved " + blk.rowId + "&" + blk.colId);
							blk.rowLabel = newlabel;
//							System.out.println("ID " + blk.rowId + "&" + blk.colId + " was in RowLeaf-" + leafLabel + " and will be put in RowLeaf-" + blk.rowLabel);		
							if (op == OpType.Write) {
								blk.data = data;
							}
						}
						retrieved.get(k/Z).add(blk);				
					}						
				}
			}
			
			System.out.println(res.size() + "");
			if(res.size() != matrix.col)
			{
//				Errors.error("[PO (rearrangeBlocksAndReturn)] Couldn't find block with id " + a + " in the path down to label " + (leafLabel - (matrix.Nrow-1))+ " or in the stash.");
				System.out.println(" ");
				System.out.println("[PO (rearrangeBlocksAndReturn)] Couldn't find block with id " + a + " in the path down to label " + (leafLabel - (matrix.Nrow-1))+ " or in the stash.");
				System.out.println(" ");
			}
			else
				System.out.println("SUCCESS: All Columns are retrieved!");
			
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
						
			System.out.println("==============================================================================");
			System.out.println("");
			System.out.println("RE-ENCRYPTION & EVICTION IS STARTED");
			System.out.println("");
			System.out.println("==============================================================================");
			v.clear();
			//======== EACH DOWNLOADED BLOCK IS EVICTED ALONG WITH MATCHING BLOCKS FROM STASH ====================================
			startEvict = System.nanoTime();
			long startEvict2 = System.nanoTime();
			ArrayList<UploadOperation>uploadList = new ArrayList<UploadOperation>();
			
			long excluded = 0;
			long excStart = 0;
			long excEnd = 0;
			
			for (int i = matrix.Drow+1; i > 0; i--) 
			{
//				ArrayList<UploadOperation>uploadList = new ArrayList<UploadOperation>();
				int row_prefix = Utils.iBitsPrefix(leafLabel+1, matrix.Drow+1, i);
				for (int m = 0; m < matrix.col; m++)
				{
					int rowNo = ((row_prefix>>(matrix.Drow+1-i))-1);
					
					excStart = System.nanoTime();
					String objectKey =  rowNo + "#" + matrix.col;
					Matrix.Bucket bucket = matrix.new Bucket(matrix.new Block());
					excEnd = System.nanoTime();
					excluded += (excEnd-excStart);
					
					excStart = System.nanoTime();
					int k = 0;
					for (int j = 0 ; j < retrieved.get(m).size() && k < Z*Z; j++) 
					{
						if (!retrieved.get(m).get(j).isDummy())
						{ 
							int jprefix = Utils.iBitsPrefix(retrieved.get(m).get(j).rowLabel+matrix.Nrow, matrix.Drow+1, i);
							
							
							if(row_prefix == jprefix) 
							{	
								bucket.blocks[k/Z][k%Z] = matrix.new Block(retrieved.get(m).get(j));
								k++;
								retrieved.get(m).get(j).erase();
							}
						}
					}			
					
					excEnd = System.nanoTime();
					excluded += (excEnd-excStart);
					
					bucket.encryptBlocks();		
										
					excStart = System.nanoTime();
					DataItem di = new SimpleDataItem(bucket.toByteArray());
					UploadOperation upload = new UploadOperation(reqID, objectKey, di);
					uploadList.add(upload);
					excEnd = System.nanoTime();
					excluded += (excEnd-excStart);
					
				}	
//				ArrayList <ScheduledOperation> sop = matrix.storedMatrix.uploadRow(uploadList);	
//				v.addAll(sop);
			}
			long endEvict2 = System.nanoTime();
			System.out.println("Select & Encrypt = " + (endEvict2-startEvict2-excluded)/ 1000000.0 + " ms");
			System.out.println(excluded/1000000.0);
			
			long startUpl = System.nanoTime();
			ArrayList <ScheduledOperation> sop = matrix.storedMatrix.uploadRowPath(uploadList);	
			long endUpl = System.nanoTime();
			System.out.println("Total Upload = " + (endUpl-startUpl)/ 1000000.0 + " ms");
			v.addAll(sop);
			Pollable.waitForCompletion(v);
			

			try {
				bw.write((int) ((endEvict2-startEvict2-excluded)/ 1000000.0)+", " );
				bw.write((int) ((endUpl-startUpl)/ 1000000.0)+"");
				totalTime += (endEvict2-startEvict2-excluded);
				totalTime += (endUpl-startUpl);
				bw2.write((int)(totalTime/1000000.0) + ", " );
			} catch (Exception e) { Errors.error(e); }
			
			for (int i = 0; i < matrix.col ; i++){
				for (int k = 0; k < retrieved.get(i).size(); k++){
					if(!retrieved.get(i).get(k).isDummy()){
						matrix.stash.colBlocks.get(i).add(retrieved.get(i).get(k));
						retrieved.get(i).get(k).erase();
					}
				}
			}
		}
		else if (dim == 1){			
			long startFind = System.nanoTime();

			// ======== OLD BLOCKS ARE ADDED ============		
			for (int i = 0; i < matrix.row; i++){
				retrieved.add(matrix.stash.rowBlocks.get(i));
				matrix.stash.rowBlocks.get(i).clear();
			}		
			//CHECK ALSO ROW STASH FOR BLOCKS
			List<Integer> path = new ArrayList<Integer>();
			int temp = leafLabel;
			while (temp >= 0){
				path.add(temp);
				temp = ((temp+1)>>1)-1;
			}
			for(int i = 0; i < path.size(); i++){
				ArrayList<Matrix.Block> colStash = matrix.stash.colBlocks.get(path.get(i));
				for (int k = 0; k < colStash.size(); k++){
					Matrix.Block blk = colStash.get(k);				
					if (!blk.isDummy()){
						if (blk.colId == a) {
							res.add(blk);
							blk.colLabel = newlabel;
							if (op == OpType.Write) {
								blk.data = data;
							}
						}
					}
				}
			}
			// ======== NEW BLOCKS ARE ADDED ============
			for (int i = 0; i < Z*(matrix.Dcol+1) ; i++) {
				for (int k = 0; k < Z*matrix.row ; k++){
					Matrix.Block blk = blocks[i][k];
					blk.dec();
					if (!blk.isDummy()){
						if (blk.colId == a) {
							res.add(blk);
//							System.out.println("SUCCESS: Block is retrieved " + blk.rowId + "&" + blk.colId);
							blk.colLabel = newlabel;
//							System.out.println("ID " + blk.rowId + "&" + blk.colId + " was in RowLeaf-" + leafLabel + " and will be put in RowLeaf-" + blk.rowLabel);		
							if (op == OpType.Write) {
								blk.data = data;
							}
						}
						retrieved.get(k/Z).add(blk);				
					}					
				}
			}
						
			System.out.println(res.size() + "");
			if(res.size() != matrix.row)
			{
//				Errors.error("[PO (rearrangeBlocksAndReturn)] Couldn't find block with id " + a + " in the path down to label " + (leafLabel - (matrix.Nrow-1))+ " or in the stash.");
				System.out.println(" ");
				System.out.println("[PO (rearrangeBlocksAndReturn)] Couldn't find block with id " + a + " in the path down to label " + (leafLabel - (matrix.Nrow-1))+ " or in the stash.");
				System.out.println(" ");
			}
			else
				System.out.println("SUCCESS: All Rows are retrieved!");
			
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
						
			System.out.println("==============================================================================");
			System.out.println("");
			System.out.println("RE-ENCRYPTION & EVICTION IS STARTED");
			System.out.println("");
			System.out.println("==============================================================================");
			v.clear();
			//======== EACH DOWNLOADED BLOCK IS EVICTED ALONG WITH MATCHING BLOCKS FROM STASH ====================================
			startEvict = System.nanoTime();
			long startEvict2 = System.nanoTime();
			ArrayList<UploadOperation>uploadList = new ArrayList<UploadOperation>();
			long excluded = 0;
			long excStart = 0;
			long excEnd = 0;
			
			for (int i = matrix.Dcol+1; i > 0; i--) 
			{
//				long startIn = System.nanoTime();
				int col_prefix = Utils.iBitsPrefix(leafLabel+1, matrix.Dcol+1, i);
				for (int m = 0; m < matrix.row; m++)
				{
					
					int colNo = ((col_prefix>>(matrix.Dcol+1-i))-1);
					excStart = System.nanoTime();
					String objectKey =  colNo + "#" + matrix.row;			
					Matrix.Bucket bucket = matrix.new Bucket(matrix.new Block());
					excEnd = System.nanoTime();
					excluded += (excEnd-excStart);
					
					excStart = System.nanoTime();
					int k = 0;
					for (int j = 0; j < retrieved.get(m).size() && k < Z*Z; j++) 
					{
						if (!retrieved.get(m).get(j).isDummy())
						{ 
							int jprefix = Utils.iBitsPrefix(retrieved.get(m).get(j).colLabel+matrix.Ncol, matrix.Dcol+1, i);
							
							if(col_prefix == jprefix) 
							{		
								bucket.blocks[k/Z][k%Z] = matrix.new Block(retrieved.get(m).get(j));
								k++;
//								System.out.println("[PO (rearrangeBlocksAndReturn)] Block with Id-" + unionList.get(j).rowId + "&" + unionList.get(j).colId + " will be re-written in bucket on the path of ColLeaf-" + unionList.get(j).colLabel + " (objectKey: " + objectKey + ")");
								retrieved.get(m).get(j).erase();							
							}
							
						}
					}	
					excEnd = System.nanoTime();
					excluded += (excEnd-excStart);
					
					bucket.encryptBlocks();	
					
					excStart = System.nanoTime();
					DataItem di = new SimpleDataItem(bucket.toByteArray());
					UploadOperation upload = new UploadOperation(reqID, objectKey, di);
					uploadList.add(upload);
					excEnd = System.nanoTime();
					excluded += (excEnd-excStart);
				}
//				long endIn = System.nanoTime();
//				System.out.println("Inside First For = " + (endIn-startIn)/ 1000000.0 + " ms");
			}
			long endEvict2 = System.nanoTime();
			System.out.println("Select & Encrypt = " + (endEvict2-startEvict2-excluded)/ 1000000.0 + " ms");
			System.out.println(excluded/1000000.0);
			
			long startUpl = System.nanoTime();
			ArrayList<ScheduledOperation> sop = matrix.storedMatrix.uploadColPath(uploadList);	
			long endUpl = System.nanoTime();
			System.out.println("uploadCol = " + (endUpl-startUpl)/ 1000000.0 + " ms");
			v.addAll(sop);
			Pollable.waitForCompletion(v);
			
			try {
				bw.write((int) ((endEvict2-startEvict2-excluded)/ 1000000.0)+", " );
				bw.write((int) ((endUpl-startUpl)/ 1000000.0)+"");
				totalTime += (endEvict2-startEvict2-excluded);
				totalTime += (endUpl-startUpl);
				bw2.write((int)(totalTime/1000000.0) + ", " );
			} catch (Exception e) { Errors.error(e); }
			
			for (int i = 0; i < matrix.row ; i++){
				for (int k = 0; k < retrieved.get(i).size(); k++){
					if(!retrieved.get(i).get(k).isDummy()){
						matrix.stash.rowBlocks.get(i).add(retrieved.get(i).get(k));
						retrieved.get(i).get(k).erase();
					}
				}
			}		
		}
				
		Pollable.waitForCompletion(v);

//		v.clear(); // wait and clear
		
		long endEvict = System.nanoTime();
		try {
//			bw.write((int) ((endEvict - startEvict) / 1000000.0)+"");
			bw.newLine();
			bw.flush();
			bw.close();
			bw2.flush();
			bw2.close();
		} catch (Exception e) { Errors.error(e); }
		
		System.out.println("==============================================================================");
		System.out.println("ALL RE-ENCRYPTION & EVICTION TOOK " + (endEvict - startEvict) / 1000000.0 + " ms"); 
		System.out.println("==============================================================================");
		System.out.println("");
//		System.out.println("CURRENT STASH SIZE = " + k);
		System.out.println("Position Map is set to " + newlabel);
		System.out.println("");
		System.out.println("==============================================================================");
		
//		if (k == matrix.stash.size) 
//		{
//			for (; j < unionList.size(); j++)
//			{ 
//				assert (unionList.get(j).isDummy()) : "Stash is overflown: " + matrix.stash.size; 
//			}	
//		}		
		
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
	ArrayList<Matrix.Block> read(long reqID, Object[] pm, int i, int dim) { return access(reqID, pm, OpType.Read, i, null, dim); }
	//========================= TREE WRITE ==================================================================
	ArrayList<Matrix.Block> write(long reqID, Object[] pm, int i, BitSet d, int dim) { return access(reqID, pm, OpType.Write, i, d, dim); }

	
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
		
		static void writePositionMap(BitSet[] map, Matrix st, int index, int val) {
			int base = (index % C) * st.Drow;
			writeBitSet(map[index/C], base, val, st.Drow);
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
		
		static int readPositionMap(BitSet[] map, Matrix st, int index) {
			int base = fastMod(index, C) * st.Drow;
			int mapIdx = fastDivide(index, C);
			if(mapIdx >= map.length) { Errors.error("Coding FAIL!"); }
			return readBitSet(map[mapIdx], base, st.Drow);
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
		
		serverMatrix.save(os);
		
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
		
		serverMatrix = new Matrix(si, is);
		
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