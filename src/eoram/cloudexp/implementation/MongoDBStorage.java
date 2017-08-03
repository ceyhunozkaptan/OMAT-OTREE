package eoram.cloudexp.implementation;

import eoram.cloudexp.artifacts.*;
import eoram.cloudexp.data.*;
import eoram.cloudexp.interfaces.*;
import eoram.cloudexp.service.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.BulkWriteResult;

import org.bson.types.Binary;


/**
 * Implements a storage interface for MongoDB.
 */
public class MongoDBStorage implements InternalStorageInterface 
{	
	private MongoClient mongoClient = null;
	private DB mongoDB = null;
	private DBCollection mongoColl = null;
	
	private String bucketName = null;
	
	private boolean resetBucket = false;

	
	public MongoDBStorage(boolean shouldReset) { resetBucket = shouldReset; }
	private char[] password = {'1','9','4','6','7','3','5'};
	private MongoCredential credential = MongoCredential.createCredential("remote","remote",password);
	@Override
	public void connect() 
	{
//		mongoClient = new MongoClient(new ServerAddress("192.168.0.11", 27017), Arrays.asList(credential)); //LOCAL HOME
//		mongoClient = new MongoClient(new ServerAddress("73.240.9.45", 27017), Arrays.asList(credential)); //HOME OLD ASUS
//		mongoClient = new MongoClient(new ServerAddress("35.166.144.249", 27017), Arrays.asList(credential)); //AMAZON EC2
		mongoClient = new MongoClient(new ServerAddress("54.244.165.88", 27017), Arrays.asList(credential)); //AMAZON EC2
//		mongoClient = new MongoClient(new ServerAddress("128.193.38.24", 27017), Arrays.asList(credential)); //BABYLON04
//		mongoClient = new MongoClient();
		
		String storagekey = SessionState.getInstance().storageKey.toLowerCase();
		String[] parts = storagekey.split("--");	
		bucketName = parts[1];
		
		mongoDB = mongoClient.getDB("remote");	
		mongoColl = mongoDB.getCollection(bucketName);
		
		ClientParameters clientParams = ClientParameters.getInstance();
		
//		long size = clientParams.maxBlocks*16*64*(clientParams.contentByteSize+18)+10000000;
		
//		mongoDB.createCollection(bucketName, new BasicDBObject("capped", true).append("size", size));
		mongoColl = mongoDB.getCollection(bucketName);
		mongoColl.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		
		
		if (resetBucket)
			mongoColl.drop();
	}

	@Override
	public ScheduledOperation downloadObject(DownloadOperation op) 
	{
		String key = op.getKey();

		String[] parts = key.split("#");
		int nodeId = Integer.parseInt(parts[1]);
				
		ScheduledOperation sop = new ScheduledOperation(op);
		{	
			try
			{			    
				BasicDBObject bucket = (BasicDBObject) mongoColl.findOne(new BasicDBObject("_id", nodeId));

				byte[] data = (byte[])bucket.get("data");
				sop.onSuccess(new SimpleDataItem(data));
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				sop.onFailure();
			}
		}
		
		sop.onFailure();
		return sop;
	}
	
	@Override
	public ScheduledOperation downloadAll(DownloadBulkOperation op) 
	{
		
		ArrayList<DataItem> dataList = new ArrayList<DataItem>();
				
		ScheduledOperation sop = new ScheduledOperation(op);
		{	
			try
			{	
				long start = System.nanoTime();
				DBCursor cursor = mongoColl.find();
				long end = 0;
				try {
					while(cursor.hasNext()) {
						BasicDBObject bucket = (BasicDBObject) cursor.next();
						byte[] data = (byte[])bucket.get("data");
						dataList.add(new SimpleDataItem(data));
					}	
					end = System.nanoTime();
//					System.out.println("Query Time = " + (end-start)/ 1000000.0 + " ms");
				} finally {
					start = System.nanoTime();
					cursor.close();
				    end = System.nanoTime();
//					System.out.println("CursorClose Time = " + (end-start)/ 1000000.0 + " ms");
				}
				
				sop.onSuccess(dataList);
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				sop.onFailure();
			}
		}
		sop.onFailure();
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> downloadPath(ArrayList<DownloadOperation> opList) 
	{
		String key = "";
		DownloadOperation op;
		String[] parts;
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		
		List<Integer> path = new ArrayList<Integer>();
		for (int i = 0; i < opList.size(); i++){
			op = opList.get(i);
			sop.add(new ScheduledOperation(op));
			key = op.getKey();
			parts = key.split("#");
			path.add(Integer.parseInt(parts[1]));	
		}
		
		try
		{	
			BasicDBObject inQuery = new BasicDBObject("_id", new BasicDBObject("$in", path));
			DBCursor cursor = mongoColl.find(inQuery);
			int i = 0;
			try {
				while(cursor.hasNext()) {
					BasicDBObject bucket = (BasicDBObject) cursor.next();
					
					byte[] data = (byte[])bucket.get("data");	
//					byte[] data = decoder.decode((bucket.getString("data")));
					
					sop.get(i).onSuccess(new SimpleDataItem(data));
					i++;
				}	
			} finally {
			    cursor.close();
			}
			return sop;
		}
		catch (NullPointerException e) { 
			for (int i = 0; i < opList.size(); i++)
				sop.get(i).onFailure();
			throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
		} 
		catch (MongoException e){
			e.printStackTrace();
			System.out.println(e.getMessage());
			for (int i = 0; i < opList.size(); i++)
				sop.get(i).onFailure();
		}
	
		for (int i = 0; i < opList.size(); i++)
			sop.get(i).onFailure();
		return sop;
	}
	
	@Override
	public ScheduledOperation downloadRow(DownloadBulkOperation op) 
	{
		long start = System.nanoTime();
		String key = op.getKey();
		String[] parts = key.split("#");
		
		int noOfCol = Integer.parseInt(parts[1]);
		int leafId = Integer.parseInt(parts[0]);

		DBObject rowBuckets = null;
		byte[] data = null;
				
		List<Integer> path = new ArrayList<Integer>();
		
		int temp = leafId;
		while (temp >= 0){
			path.add(temp);
			temp = ((temp+1)>>1)-1;
		}
		
		ArrayList<DataItem> dataList = new ArrayList<DataItem>();
				
		ScheduledOperation sop = new ScheduledOperation(op);
		long end = System.nanoTime();
		System.out.println("Initial Time = " + (end-start)/ 1000000.0 + " ms");
		{	
			try
			{	
				BasicDBObject inQuery = new BasicDBObject("_id", new BasicDBObject("$in", path));
				start = System.nanoTime();
				DBCursor cursor = mongoColl.find(inQuery);
				end = System.nanoTime();
				System.out.println("Query Time = " + (end-start)/ 1000000.0 + " ms");
				try {
					while(cursor.hasNext()) {
						start = System.nanoTime();
//						rowBuckets = (BasicDBObject) cursor.next();
						rowBuckets = cursor.next();
						end = System.nanoTime();
//						System.out.println("CursorNext Time = " + (end-start)/ 1000000.0 + " ms");
						start = System.nanoTime();
						for (int i = 0; i < noOfCol; i++){
							
							data = (byte[])rowBuckets.get(""+i);
							
//							data = decoder.decode((rowBuckets.getString(""+i)));		
							dataList.add(new SimpleDataItem(data));
						}
						end = System.nanoTime();
//						System.out.println("AllColumnRead Time = " + (end-start)/ 1000000.0 + " ms");
					}	
				} finally {
					start = System.nanoTime();
					cursor.close();
				    end = System.nanoTime();
//					System.out.println("CursorClose Time = " + (end-start)/ 1000000.0 + " ms");
				}
				
				sop.onSuccess(dataList);
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				sop.onFailure();
			}
		}
		sop.onFailure();
		return sop;
	}
	
	// FIXME
	@Override
	public ArrayList<ScheduledOperation> downloadRow(ArrayList<DownloadOperation> opList) 
	{
		String key = "";
		DownloadOperation op;
		String[] parts;
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		int noOfCol = 0;
		byte[] data = null;
		
		
		List<Integer> path = new ArrayList<Integer>();
		for (int i = 0; i < opList.size(); i++){
			op = opList.get(i);
			sop.add(new ScheduledOperation(op));
			key = op.getKey();
			parts = key.split("#");
			noOfCol = Integer.parseInt(parts[1]);	
			path.add(Integer.parseInt(parts[0]));				
		}
		
		try
		{	
			BasicDBObject inQuery = new BasicDBObject("_id", new BasicDBObject("$in", path));
			DBCursor cursor = mongoColl.find(inQuery);

			try {
				while(cursor.hasNext()) {
					BasicDBObject rowBuckets = (BasicDBObject) cursor.next();
					for (int i = 0; i < noOfCol; i++){

						data = (byte[])rowBuckets.get(""+i);
//						data = decoder.decode((rowBuckets.getString(""+i)));								
					}
				}	   
			} finally {
			    cursor.close();
			}
			     
			return sop;
		}
		catch (NullPointerException e) { 
			for (int i = 0; i < opList.size(); i++)
				sop.get(i).onFailure();
			throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
		} 
		catch (MongoException e){
			e.printStackTrace();
			System.out.println(e.getMessage());
			for (int i = 0; i < opList.size(); i++)
				sop.get(i).onFailure();
		}
	
		for (int i = 0; i < opList.size(); i++)
			sop.get(i).onFailure();
		return sop;
	}
	
	@Override
	public ScheduledOperation downloadCol(DownloadBulkOperation op) 
	{
		String key = op.getKey();
		String[] parts = key.split("#");
		
		int leafId = Integer.parseInt(parts[0]);
		int row = Integer.parseInt(parts[1]);
		
		byte[] data = null;
		
		List<Integer> path = new ArrayList<Integer>();
		
		int temp = leafId;
		while (temp >= 0){
			path.add(temp);
			temp = ((temp+1)>>1)-1;
		}
		
		ArrayList<DataItem> dataList = new ArrayList<DataItem>();
				
		ScheduledOperation sop = new ScheduledOperation(op);
		{	
			try
			{	
				BasicDBObject query = new BasicDBObject();
				BasicDBObject fields = new BasicDBObject();
				for (int i = 0; i < path.size(); i++){
					fields.append(path.get(i)+"", "1");
				}
								
				DBCursor cursor = mongoColl.find(query, fields);
				
				try {
					while(cursor.hasNext()) {
						BasicDBObject rowBuckets = (BasicDBObject) cursor.next();
						for (int i = 0; i < path.size(); i++){
							data = (byte[])rowBuckets.get(""+path.get(i));
//							data = decoder.decode((rowBuckets.getString(""+path.get(i))));				
							dataList.add(new SimpleDataItem(data));
						}
					}	
				} finally {
				    cursor.close();
				}
				
				sop.onSuccess(dataList);
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				sop.onFailure();
			}
		}
		sop.onFailure();
		return sop;
	}
	
	@Override
	public ScheduledOperation uploadObject(UploadOperation op) 
	{
		String key = op.getKey();
		byte[] data = op.getDataItem().getData();
		
		Binary dataBin = new Binary(data);	
//		String dataStr = encoder.encodeToString(data);
		
		
		String[] parts = key.split("#");
		int nodeId = Integer.parseInt(parts[1]);
							
		ScheduledOperation sop = new ScheduledOperation(op);
		{	
			try
			{			    
			    BasicDBObject doc = new BasicDBObject("_id", nodeId)
		                .append("data", dataBin);
			    
			    mongoColl.update(new BasicDBObject("_id", nodeId),doc,true,false);		    
//			    mongoColl.createIndex(new BasicDBObject("_id",1));
			    
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				sop.onFailure();
			}
		}	
		sop.onFailure();
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadPath(ArrayList<UploadOperation> opList) 
	{
		BulkWriteOperation builder = mongoColl.initializeUnorderedBulkOperation();
		String key = "";
		UploadOperation op;
		String[] parts;
		int nodeId;
	
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		{	
			try
			{	
				for (int i = 0; i < opList.size(); i++){
					op = opList.get(i);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					nodeId = Integer.parseInt(parts[1]);				
					DataItem data = op.getDataItem();
					Binary dataBin = new Binary(data.getData());
//					String dataStr = encoder.encodeToString(data.getData());
														
					BasicDBObject updateQuery = new BasicDBObject("$set",
							new BasicDBObject("data", dataBin));
					
					
					builder.find(new BasicDBObject("_id", nodeId)).updateOne(updateQuery);
				}	        
				builder.execute(WriteConcern.ACKNOWLEDGED);
//				mongoColl.createIndex(new BasicDBObject("_id",1));
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				System.out.println(e.getMessage());
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
			}
		}		
		for (int i = 0; i < opList.size(); i++)
			sop.get(i).onFailure();
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadAll(ArrayList<UploadOperation> opList) 
	{
		BulkWriteOperation builder = mongoColl.initializeUnorderedBulkOperation();
		String key = "";
		UploadOperation op;
		String[] parts;
		int nodeId;
		DataItem data = null;
		String dataStr = null;
		Binary dataBin = null;
		BasicDBObject updateQuery = null;
		
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		{	
			try
			{	
				for (int i = 0; i < opList.size(); i++){
					op = opList.get(i);
					opList.set(i, null);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					nodeId = Integer.parseInt(parts[1]);				
					data = op.getDataItem();
					op = null;
					
					
					
					dataBin = new Binary(data.getData());
//					dataStr = encoder.encodeToString(data.getData());
														
					updateQuery = new BasicDBObject("$set",
							new BasicDBObject("data", dataBin));
					
					builder.find(new BasicDBObject("_id", nodeId)).upsert().updateOne(updateQuery);
					updateQuery = null;
				}	  
//				System.gc();
				System.out.println("opList is cleared!!!!!!!!!!!!!!!!!!!!!!!");
				builder.execute(WriteConcern.UNACKNOWLEDGED);
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				opList = null;
				System.gc();
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				System.out.println(e.getMessage());
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
			}
		}		
		for (int i = 0; i < opList.size(); i++)
			sop.get(i).onFailure();
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadRow(ArrayList<UploadOperation> opList)
	{	
		String key = "";
		UploadOperation op;
		String[] parts;
		int rowId;
		int colId;
		op = opList.get(0);
		key = op.getKey();
		parts = key.split("#");
		rowId = Integer.parseInt(parts[0]);
		BasicDBObject doc = new BasicDBObject("_id", rowId);
		String dataStr = "";
		Binary dataBin = null;
		DataItem data = null;

		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();

		{	
			try
			{	
				for (int i = 0; i < opList.size(); i++){
					op = opList.get(i);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					rowId = Integer.parseInt(parts[0]);
					colId = Integer.parseInt(parts[1]);				
					data = op.getDataItem();
					
					
					dataBin = new Binary(data.getData());
//					dataStr = encoder.encodeToString(data.getData());
					
					doc.append(""+colId, dataBin);
//					System.out.println(colId);
				}	        
				long start = System.nanoTime();
				mongoColl.update(new BasicDBObject("_id", rowId),doc,true,false);
				long end = System.nanoTime();
//				System.out.println("Update (UploadRow) = " + (end-start)/ 1000000.0 + " ms");
//				mongoColl.createIndex(new BasicDBObject("_id",1));
				
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				System.out.println(e.getMessage());
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
			}
		}		
		for (int i = 0; i < opList.size(); i++)
			sop.get(i).onFailure();
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadAllRow(ArrayList<UploadOperation> opList)
	{	
		BulkWriteOperation builder = mongoColl.initializeUnorderedBulkOperation();
		String key = "";
		UploadOperation op;
		String[] parts;
		int rowId;
		int NoOfcol;
		op = opList.get(0);
		key = op.getKey();
		parts = key.split("#");
		rowId = Integer.parseInt(parts[0]);
		NoOfcol = Integer.parseInt(parts[1]);
		BasicDBObject doc = new BasicDBObject("_id", rowId);
		DataItem data = null;
		Binary dataBin = null;
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();

		{	
			try
			{	
				for (int i = 0; i < opList.size(); i++){
					op = opList.get(i);
					opList.set(i, null);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					rowId = Integer.parseInt(parts[0]);				
					data = op.getDataItem();
					
					dataBin = new Binary(data.getData());
					
					if(i%NoOfcol == 0){
						doc = new BasicDBObject();
					}		
					
					doc.append(""+i%NoOfcol, dataBin);
					
					if(i%NoOfcol == NoOfcol-1){
						builder.find(new BasicDBObject("_id", rowId)).upsert().updateOne(new BasicDBObject("$set",doc));
						doc = null;
					}
													
				}	  
				long start = System.nanoTime();
				builder.execute(WriteConcern.UNACKNOWLEDGED);
				builder = null;
				doc = null;
				data = null;
				long end = System.nanoTime();
				System.out.println("Upload (UploadAllData) = " + (end-start)/ 1000000.0 + " ms");
				System.gc();
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				
				opList.clear();
				opList = null;
				System.gc();
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				System.out.println(e.getMessage());
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
			}
		}		
		for (int i = 0; i < opList.size(); i++)
			sop.get(i).onFailure();
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadRowPath(ArrayList<UploadOperation> opList)
	{	
		BulkWriteOperation builder = mongoColl.initializeUnorderedBulkOperation();
		String key = "";
		UploadOperation op;
		String[] parts;
		int rowId;
		op = opList.get(0);
		key = op.getKey();
		parts = key.split("#");
		int NoOfcol = Integer.parseInt(parts[1]);
		BasicDBObject doc = new BasicDBObject();
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		DataItem data = null;
		Binary dataBin = null;
		String dataStr = null;

		{	
			try
			{	
				for (int i = 0; i < opList.size(); i++){
					op = opList.get(i);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					rowId = Integer.parseInt(parts[0]);
//					colId = Integer.parseInt(parts[1]);				
					data = op.getDataItem();
					dataBin = new Binary(data.getData());
					
//					dataStr = encoder.encodeToString(data.getData());
					
					if(i%NoOfcol == 0){
						doc = new BasicDBObject();
					}		
					
//					doc.append(""+i%NoOfcol, dataStr);
					doc.append(""+i%NoOfcol, dataBin);
					
					if(i%NoOfcol == NoOfcol-1){
						builder.find(new BasicDBObject("_id", rowId)).upsert().updateOne(new BasicDBObject("$set",doc));
					}
				}	        
				long start = System.nanoTime();
				BulkWriteResult results = builder.execute(WriteConcern.UNACKNOWLEDGED);
//				mongoColl.update(new BasicDBObject("_id", rowId),doc,true,false);
				long end = System.nanoTime();
				
//				System.out.println("Update (UploadRow) = " + (end-start)/ 1000000.0 + " ms");
//				mongoColl.createIndex(new BasicDBObject("_id",1));
				
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				System.out.println(e.getMessage());
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
			}
		}		
		for (int i = 0; i < opList.size(); i++)
			sop.get(i).onFailure();
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadColPath(ArrayList<UploadOperation> opList) 
	{
		long start = System.nanoTime();
		BulkWriteOperation builder = mongoColl.initializeUnorderedBulkOperation();
		String key = "";
		UploadOperation op;
		String[] parts;
		key = opList.get(0).getKey();
		parts = key.split("#");
		int NoOfrow = Integer.parseInt(parts[1]);
		int colId;
		DataItem data = null;
		String dataStr = null;
		Binary dataBin = null;
		
		ArrayList<BasicDBObject> updateQueryList = new ArrayList<BasicDBObject>(NoOfrow);
		for (int i=0; i < NoOfrow; i++)
			updateQueryList.add(new BasicDBObject());

		long end = System.nanoTime();
		System.out.println("Initial Time (Upload) = " + (end-start)/ 1000000.0 + " ms");
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		{	
			try
			{	
				start = System.nanoTime();
//				for (int i = 0; i < opList.size(); i++){
				for (int i = opList.size()-1; i >= 0; i--){
					op = opList.get(i);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					colId = Integer.parseInt(parts[0]);				
					data = op.getDataItem();
					
					dataBin = new Binary(data.getData());
					
//					if (i % NoOfrow == 0){
//						updateQueryList.add(new BasicDBObject(""+colId, dataBin));
//					}else{
					updateQueryList.get(i % NoOfrow).append(""+colId, dataBin);
//					}
					
					
				}	  
				
				for(int i = 0; i < NoOfrow; i++){
					builder.find(new BasicDBObject("_id", i)).upsert().updateOne(new BasicDBObject("$set",updateQueryList.get(i)));
				}
				end = System.nanoTime();
//				System.out.println("Middle Step (Upload) = " + (end-start)/ 1000000.0 + " ms");
				
				start = System.nanoTime();
				BulkWriteResult results = builder.execute(WriteConcern.UNACKNOWLEDGED);
				end = System.nanoTime();
				System.out.println("Builder (Upload) = " + (end-start)/ 1000000.0 + " ms");

				start = System.nanoTime();
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				System.out.println(e.getMessage());
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
			}
		}		
		for (int i = 0; i < opList.size(); i++)
			sop.get(i).onFailure();
		return sop;
	}

	@Override
	public ScheduledOperation deleteObject(DeleteOperation op) 
	{
		String key = op.getKey();
		
		ScheduledOperation sop = new ScheduledOperation(op);
		{	
			try
			{
				mongoColl.remove(new BasicDBObject("_id", key));

				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				sop.onFailure();
			}
		}
		
		sop.onFailure();
		return sop;
	}
	
	@Override
	public ScheduledOperation copyObject(CopyOperation op) 
	{
		String srcKey = op.getSourceKey();
		String destKey = op.getDestinationKey();
		
		
		ScheduledOperation sop = new ScheduledOperation(op);
		{	
			try
			{
				DBObject src = mongoColl.findOne(new BasicDBObject("_id", srcKey));
				BasicDBObject dst = new BasicDBObject()
						.append("_id", destKey)
						.append("data", src.get("data"));
				
				mongoColl.insert(dst);
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (MongoException e){
				e.printStackTrace();
				sop.onFailure();
			}
		}
		
		sop.onFailure();
		return sop;
	}
	
	@Override
	public ScheduledOperation listObjects(ListOperation op) 
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		List<String> ret = new ArrayList<String>();
		try
		{
			List<DBObject> all = mongoColl.find().toArray();

			
			for(DBObject os : all) { ret.add((String)os.get("_id")); }
	
			
			sop.onSuccess(new WrappedListDataItem(ret));
		}
		catch (NullPointerException e) { 
			sop.onFailure();
			throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
		} 
		catch (MongoException e){
			e.printStackTrace();
			sop.onFailure();
		}
			
		return sop;
	}

	@Override
	public void disconnect() { mongoClient.close(); }
	
	@Override
	public long totalByteSize()
	{
		long ret = 0;
//		List<DBObject> all = mongoColl.find().toArray();
		
//		for(DBObject os : all) { ret += DefaultDBEncoder.FACTORY.create().writeObject(new BasicOutputBuffer(), os); }
		
		return ret;
	}

	@Override
	public void cloneTo(String to) 
	{
		
	}

	@Override
	public ArrayList<ScheduledOperation> downloadCol(ArrayList<DownloadOperation> op) {
		// TODO Auto-generated method stub
		return null;
	}
}
