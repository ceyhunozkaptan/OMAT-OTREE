package eoram.cloudexp.implementation;

import eoram.cloudexp.artifacts.*;
import eoram.cloudexp.data.*;
import eoram.cloudexp.interfaces.*;
import eoram.cloudexp.service.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;

import org.bson.Document;
import org.bson.types.Binary;


/**
 * Implements a storage interface for MongoDB.
 */
public class MongoDBStorage2 implements InternalStorageInterface 
{	
	private MongoClient mongoClient = null;
	private MongoDatabase mongoDB = null;
	private MongoCollection<Document> mongoColl = null;
	
	private String bucketName = null;
	
	private boolean resetBucket = false;
	
	public MongoDBStorage2(boolean shouldReset) { resetBucket = shouldReset; }
	private char[] password = {'1','9','4','6','7','3','5'};
	private MongoCredential credential = MongoCredential.createCredential("remote","remote",password);
	@Override
	public void connect() 
	{
//		mongoClient = new MongoClient(new ServerAddress("192.168.0.11", 27017), Arrays.asList(credential)); //LOCAL HOME
//		mongoClient = new MongoClient(new ServerAddress("73.240.9.45", 27017), Arrays.asList(credential)); //HOME OLD ASUS
//		mongoClient = new MongoClient(new ServerAddress("35.166.144.249", 27017), Arrays.asList(credential)); //AMAZON EC2
//		mongoClient = new MongoClient(new ServerAddress("54.89.252.192", 27017), Arrays.asList(credential)); //AMAZON EC2
		mongoClient = new MongoClient(new ServerAddress("128.193.38.24", 27017), Arrays.asList(credential)); //BABYLON04
//		mongoClient = new MongoClient();
		
		String storagekey = SessionState.getInstance().storageKey.toLowerCase();
		String[] parts = storagekey.split("--");	
		bucketName = parts[1];
		
		mongoDB = mongoClient.getDatabase("remote");
		mongoDB.withWriteConcern(WriteConcern.UNACKNOWLEDGED);

		mongoDB.createCollection(bucketName, new CreateCollectionOptions().capped(true).sizeInBytes(153224901));
		mongoColl = mongoDB.getCollection(bucketName);
		mongoColl.withWriteConcern(WriteConcern.UNACKNOWLEDGED);
		
//		if (resetBucket)
//			mongoColl.drop();
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
				Document bucket = mongoColl.find(new Document("_id", nodeId)).first();

				byte[] data = (byte[])bucket.get("data");
//				byte[] data = decoder.decode((bucket.getString("data")));
				sop.onSuccess(new SimpleDataItem(data));
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
				MongoCursor<Document> cursor = mongoColl.find().iterator();
				long end = 0;
				try {
					while(cursor.hasNext()) {
						Document bucket = cursor.next();
						byte[] data = (byte[])bucket.get("data");
//						byte[] data = decoder.decode((bucket.getString("data")));
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
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
			Document inQuery = new Document("_id", new Document("$in", path));
			MongoCursor<Document> cursor = mongoColl.find(inQuery).iterator();
			int i = 0;
			try {
				while(cursor.hasNext()) {
					Document bucket = cursor.next();
					
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
			throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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

		Document rowBuckets = null;
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
				Document inQuery = new Document("_id", new Document("$in", path));
				start = System.nanoTime();
				MongoCursor<Document> cursor = mongoColl.find(inQuery).iterator();
				end = System.nanoTime();
				System.out.println("Query Time = " + (end-start)/ 1000000.0 + " ms");
				try {
					while(cursor.hasNext()) {
						start = System.nanoTime();
//						rowBuckets = (Document) cursor.next();
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
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
			Document inQuery = new Document("_id", new Document("$in", path));
			MongoCursor<Document> cursor = mongoColl.find(inQuery).iterator();

			try {
				while(cursor.hasNext()) {
					Document rowBuckets = cursor.next();
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
			throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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

				ArrayList<String> fields = new ArrayList<>();
				for (int i = 0; i < path.size(); i++){
					fields.add(path.get(i)+"");
				}
				
				
//				MongoCursor<Document> cursor = mongoColl.find(fields).iterator();
				MongoCursor<Document> cursor = mongoColl.find().projection(Projections.include(fields)).iterator();
				
				try {
					while(cursor.hasNext()) {
						Document rowBuckets = cursor.next();
						for (int i = 0; i < path.size(); i++){
							data = ((Binary)rowBuckets.get(""+path.get(i))).getData();
			
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
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
			    Document doc = new Document("_id", nodeId)
		                .append("data", dataBin);
			    
			    mongoColl.updateOne(Filters.eq("_id", nodeId),doc,new UpdateOptions().upsert(true).bypassDocumentValidation(true));		    
//			    mongoColl.createIndex(new Document("_id",1));
			    
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
//		BulkWriteOperation builder = mongoColl.initializeUnorderedBulkOperation();
		String key = "";
		UploadOperation op;
		String[] parts;
		int nodeId;
		
		ArrayList<UpdateOneModel<Document>> operations = new ArrayList<UpdateOneModel<Document>>();
		
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
					Document updateQuery = new Document("$set",
							new Document("data", dataBin));
					
					operations.add(new UpdateOneModel<>(new Document("_id", nodeId), updateQuery ,new UpdateOptions().upsert(true)));
					
							
//					builder.find(new Document("_id", nodeId)).updateOne(updateQuery);
				}	        
//				builder.execute(WriteConcern.UNACKNOWLEDGED);
//				mongoColl.createIndex(new Document("_id",1));
				mongoColl.bulkWrite(operations,new BulkWriteOptions().ordered(false));
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
//		BulkWriteOperation builder = mongoColl.initializeUnorderedBulkOperation();
		String key = "";
		UploadOperation op;
		String[] parts;
		int nodeId;
		DataItem data = null;
	
		Document updateQuery = null;
		
		ArrayList<UpdateOneModel<Document>> operations = new ArrayList<UpdateOneModel<Document>>();
		
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
					data = op.getDataItem();
					
					Binary dataBin  = new Binary(data.getData());
														
					updateQuery = new Document("$set",
							new Document("data", dataBin));
					
					operations.add(new UpdateOneModel<>(new Document("_id", nodeId), updateQuery, new UpdateOptions().upsert(true)));
					
//					builder.find(new Document("_id", nodeId)).upsert().updateOne(updateQuery);
					updateQuery = null;
				}	    
				mongoColl.bulkWrite(operations,new BulkWriteOptions().ordered(false));
				
//				builder.execute(WriteConcern.UNACKNOWLEDGED);
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
		Document doc = new Document("_id", rowId);
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
				mongoColl.updateOne(Filters.eq("_id", rowId),doc,new UpdateOptions().upsert(true).bypassDocumentValidation(true));		
				long end = System.nanoTime();
//				System.out.println("Update (UploadRow) = " + (end-start)/ 1000000.0 + " ms");
//				mongoColl.createIndex(new Document("_id",1));
				
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
//		BulkWriteOperation builder = mongoColl.initializeUnorderedBulkOperation();
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
		Document doc = new Document("_id", rowId);
		DataItem data = null;

		ArrayList<UpdateOneModel<Document>> operations = new ArrayList<UpdateOneModel<Document>>();
		
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
					data = op.getDataItem();
					
					Binary dataBin = new Binary(data.getData());

					if(i%NoOfcol == 0){
						doc = new Document();
					}		
					
					doc.append(""+i%NoOfcol, dataBin);
					
					if(i%NoOfcol == NoOfcol-1){
						operations.add(new UpdateOneModel<>(new Document("_id", rowId), new Document("$set", doc), new UpdateOptions().upsert(true)));
//						builder.find(new Document("_id", rowId)).upsert().updateOne(new Document("$set",doc));
						doc = null;
					}
													
				}	  
				long start = System.nanoTime();
				mongoColl.bulkWrite(operations,new BulkWriteOptions().ordered(false).bypassDocumentValidation(true));
//				builder.execute(WriteConcern.UNACKNOWLEDGED);
//				mongoColl.update(new Document("_id", rowId),doc,true,false);
				long end = System.nanoTime();
				System.out.println("Upload (UploadAllData) = " + (end-start)/ 1000000.0 + " ms");
//				mongoColl.createIndex(new Document("_id",1));
				System.gc();
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
//		BulkWriteOperation builder = mongoColl.initializeUnorderedBulkOperation();
		String key = "";
		UploadOperation op;
		String[] parts;
		int rowId;
		op = opList.get(0);
		key = op.getKey();
		parts = key.split("#");
		int NoOfcol = Integer.parseInt(parts[1]);
		Document doc = new Document();
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		DataItem data = null;
		Binary dataBin = null;
		
		ArrayList<UpdateOneModel<Document>> operations = new ArrayList<UpdateOneModel<Document>>();

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
					
					if(i%NoOfcol == 0){
						doc = new Document();
					}		

					doc.append(""+i%NoOfcol, dataBin);
					
					if(i%NoOfcol == NoOfcol-1){
						operations.add(new UpdateOneModel<>(new Document("_id", rowId), new Document("$set", doc),new UpdateOptions().upsert(true)));
//						builder.find(new Document("_id", rowId)).upsert().updateOne(new Document("$set",doc));
					}
				}	        
				long start = System.nanoTime();
//				builder.execute(WriteConcern.UNACKNOWLEDGED);
				mongoColl.bulkWrite(operations,new BulkWriteOptions().ordered(false));
//				mongoColl.update(new Document("_id", rowId),doc,true,false);
				long end = System.nanoTime();
				System.out.println("Update (UploadRow) = " + (end-start)/ 1000000.0 + " ms");
//				mongoColl.createIndex(new Document("_id",1));
				
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
//		BulkWriteOperation builder = mongoColl.initializeUnorderedBulkOperation();
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
		
		ArrayList<UpdateOneModel<Document>> operations = new ArrayList<UpdateOneModel<Document>>();
		
		ArrayList<Document> updateQueryList = new ArrayList<Document>(NoOfrow);
		for (int i=0; i < NoOfrow; i++)
			updateQueryList.add(new Document());
	
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

			
					updateQueryList.get(i % NoOfrow).append(""+colId, dataBin);
					
//					Document updateQuery = new Document("$set",
//							new Document(""+colId, dataStr));
				}	  
				
				for(int i = 0; i < NoOfrow; i++){
					operations.add(new UpdateOneModel<>(new Document("_id", i), new Document("$set", updateQueryList.get(i))));
//					builder.find(new Document("_id", i)).upsert().updateOne(new Document("$set",updateQueryList.get(i)));
				}
				end = System.nanoTime();
//				System.out.println("Middle Step (Upload) = " + (end-start)/ 1000000.0 + " ms");
				
				start = System.nanoTime();
				mongoColl.bulkWrite(operations,new BulkWriteOptions().ordered(false).bypassDocumentValidation(true));
//				builder.execute(WriteConcern.UNACKNOWLEDGED);
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
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
				mongoColl.deleteOne(new Document("_id", key));

				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
				Document src = mongoColl.find(new Document("_id", srcKey)).first();
				Document dst = new Document()
						.append("_id", destKey)
						.append("data", src.get("data"));
				
				mongoColl.insertOne(dst);
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
//			List<DBObject> all = mongoColl.find().toArray();

			
//			for(DBObject os : all) { ret.add((String)os.get("_id")); }
	
			
			sop.onSuccess(new WrappedListDataItem(ret));
		}
		catch (NullPointerException e) { 
			sop.onFailure();
			throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getNamespace(), e);
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
