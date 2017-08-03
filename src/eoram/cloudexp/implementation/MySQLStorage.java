package eoram.cloudexp.implementation;

import eoram.cloudexp.artifacts.*;
import eoram.cloudexp.data.*;
import eoram.cloudexp.interfaces.*;
import eoram.cloudexp.service.*;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.PreparedStatement;


/**
 * Implements a storage interface for MySQL.
 * 
 */
public class MySQLStorage implements InternalStorageInterface 
{
	
	private Connection con = null;
	private Statement st = null;
	
//	private String url = "jdbc:mysql://localhost:3306/remote";
//	private String url = "jdbc:mysql://192.168.0.11:3306/remote";
//	private String url = "jdbc:mysql://54.244.175.226:3306/remote?rewriteBatchedStatements=true"; //amazon ec2
	private String url = "jdbc:mysql://128.193.38.24:4450/remote?rewriteBatchedStatements=true"; //babylon 04
    private String user = "remote";
    private String password = "1946735";
	
	private String bucketName = null;
	
	private boolean resetBucket = false;

	public MySQLStorage(boolean shouldReset) { resetBucket = shouldReset; }

	@Override
	public void connect() 
	{		
		String storagekey = SessionState.getInstance().storageKey.toLowerCase();
		String[] parts = storagekey.split("--");
		
		bucketName = parts[1];
		try {
			con = DriverManager.getConnection(url, user, password);
//			con.setAutoCommit(false);
			st = con.createStatement();
			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			String sqlORAM = "create table " + bucketName + " ("
	        		+ "rowId int unsigned not null,"
	        		+ "data varbinary(16384) not null,"
	        		+ "primary key (rowId)"
	        		+ ")"; 
			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			int NoOfcol = 64;
			String sqlMatrix = "create table " + bucketName + " ("
	        		+ "rowId int unsigned not null,";		
			for (int i = 0; i < NoOfcol-1; i++){
				sqlMatrix += " col" + i + " blob not null,";
			}
			sqlMatrix += " primary key (rowId)";
			sqlMatrix += ")";
			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			st.executeUpdate(sqlORAM); //st.executeUpdate(sqlORAM); st.executeUpdate(sqlMatrix);
			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			
			
//			con.commit();
			if (resetBucket){
				sqlORAM = "delete from " + bucketName;
				st.executeUpdate(sqlORAM);
//				con.commit();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}  
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
				String query = "SELECT data FROM " + bucketName + " WHERE rowId=" + nodeId;
				ResultSet rs = st.executeQuery(query);
				byte[] data = null;
				
				while (rs.next()) {
					data = rs.getBytes("data");
				}
				rs.close();
				
				sop.onSuccess(new SimpleDataItem(data));
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
			} 
			catch (SQLException e) {
				e.printStackTrace();
			}
		}		
		sop.onFailure();
		return sop;
	}
	
	@Override
	public ScheduledOperation downloadAll(DownloadBulkOperation op) {
		ArrayList<DataItem> dataList = new ArrayList<DataItem>();
		byte[] data = null;
			
		ScheduledOperation sop = new ScheduledOperation(op);
		{	
			try
			{	
				long start = System.nanoTime();
				String query = "SELECT * FROM " + bucketName;
				ResultSet rs = st.executeQuery(query);
				
				while (rs.next()) {
					String id = rs.getString("rowId");
					data = rs.getBytes("data");
					dataList.add(new SimpleDataItem(data));
				}
				long end = System.nanoTime();
//				System.out.println("Query Time = " + (end-start)/ 1000000.0 + " ms");
				rs.close();
				
				sop.onSuccess(dataList);
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
			} 
			catch (SQLException e) {
				e.printStackTrace();
			}
		}		
		sop.onFailure();
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
				String query = "SELECT * FROM " + bucketName + " WHERE rowId IN (" + path.toString().substring(1,path.toString().length()-1) + ")";
				ResultSet rs = st.executeQuery(query);
				try {
					while(rs.next()) {					
						start = System.nanoTime();
						for (int i = 0; i < noOfCol; i++){
							Blob blob = rs.getBlob("col"+i);

							int blobLength = (int) blob.length();  
							byte[] data = blob.getBytes(1, blobLength);
							blob.free();
											
							dataList.add(new SimpleDataItem(data));
						}
						end = System.nanoTime();
						System.out.println("AllColumnRead Time = " + (end-start)/ 1000000.0 + " ms");
					}	
				} finally {
					rs.close();
				}
				sop.onSuccess(dataList);
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
			} 
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
		sop.onFailure();
		return sop;
	}
	
	@Override
	public ScheduledOperation downloadCol(DownloadBulkOperation op) 
	{
		String key = op.getKey();
		String[] parts = key.split("#");
		
		int leafId = Integer.parseInt(parts[0]);
		int row = Integer.parseInt(parts[1]);
		
		List<String> path = new ArrayList<String>();
		
		int temp = leafId;
		while (temp >= 0){
			path.add("col" + temp);
			temp = ((temp+1)>>1)-1;
		}
		
		ArrayList<DataItem> dataList = new ArrayList<DataItem>();		
		ScheduledOperation sop = new ScheduledOperation(op);
		{	
			try
			{			    
				String query = "SELECT " + path.toString().substring(1,path.toString().length()-1) + " FROM " + bucketName;
				ResultSet rs = st.executeQuery(query);
				try {
					while (rs.next()) {
						for (int i = 0; i < path.size(); i++){
							Blob blob = rs.getBlob(path.get(i));
	
							int blobLength = (int) blob.length();  
							byte[] data = blob.getBytes(1, blobLength);
							blob.free();
							
							dataList.add(new SimpleDataItem(data));
						}
					}
				} finally {
					rs.close();
				}	
				sop.onSuccess(dataList);
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
			} 
			catch (SQLException e) {
				e.printStackTrace();
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
		String[] parts = key.split("#");
		int nodeId = Integer.parseInt(parts[1]);
						
		ScheduledOperation sop = new ScheduledOperation(op);
		{	
			try
			{			    
				String query = "insert into " + bucketName + " (rowId, data)"
				        + " values (?, ?)"
				        + " ON DUPLICATE KEY UPDATE"
				        + " rowId=values(rowId), data=values(data)";
				
				PreparedStatement preparedStmt = (PreparedStatement) con.prepareStatement(query);
				preparedStmt.setInt(1, nodeId);
				preparedStmt.setBinaryStream(2,new ByteArrayInputStream(data),data.length);
				preparedStmt.executeUpdate();

//				con.commit();
				
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
			} 
			catch (SQLException e) {
				e.printStackTrace();
				sop.onFailure();
			}
		}	
		sop.onFailure();
		return sop;
	}
	

	@Override
	public ArrayList<ScheduledOperation> uploadAll(ArrayList<UploadOperation> opList) {
		String key = "";
		UploadOperation op;
		byte[] data = null;
		String[] parts;
		int nodeId;
						
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		{	
			try
			{			    
				String query = "insert into " + bucketName + " (rowId, data)"
				        + " values (?, ?)"
				        + " ON DUPLICATE KEY UPDATE"
				        + " rowId=values(rowId), data=values(data)";
				
				PreparedStatement preparedStmt = (PreparedStatement) con.prepareStatement(query);
				
				for (int i = 0; i < opList.size(); i++){
					op = opList.get(i);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					nodeId = Integer.parseInt(parts[1]);				
					data = op.getDataItem().getData();
					
					preparedStmt.setInt(1, nodeId);
					preparedStmt.setBinaryStream(2,new ByteArrayInputStream(data),data.length);
					preparedStmt.addBatch();
				}
				preparedStmt.executeBatch();
//				con.commit();
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
			} 
			catch (SQLException e) {
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
	public ArrayList<ScheduledOperation> uploadAllRow(ArrayList<UploadOperation> opList) {
		String key = "";
		UploadOperation op;
		String[] parts;
		int rowId;
		op = opList.get(0);
		key = op.getKey();
		parts = key.split("#");
		int NoOfcol = Integer.parseInt(parts[1]);
		DataItem data = null;
		System.out.println(NoOfcol);
		
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		{	
			try
			{				
				String query = "insert into " + bucketName;
				String columns = " (rowId, ";
				String values = " values (?, ";
				String update = " ON DUPLICATE KEY UPDATE";
				String last = " rowId=values(rowId), ";
				for (int i = 0; i < NoOfcol; i++){
					if (i == NoOfcol-1){
						columns += "col" + i + ")";
						values += "?)";
						last += "col" + i + "=values(col" + i + ")";
					}
					else {
						columns += "col" + i + ", ";
						values += "?, ";
						last += "col" + i + "=values(col" + i + "), ";
					}			
				}
				String wholeQuery = query+columns+values+update+last;
				PreparedStatement preparedStmt = (PreparedStatement) con.prepareStatement(wholeQuery);
						
				for (int i = 0; i < opList.size(); i++){
					op = opList.get(i);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					rowId = Integer.parseInt(parts[0]);
			
					data = op.getDataItem();
				
					if(i%NoOfcol == 0){
						preparedStmt.setInt(1, rowId);
					}
					
					preparedStmt.setBinaryStream(i%NoOfcol+2,new ByteArrayInputStream(data.getData()),data.getData().length);
					
					if(i%NoOfcol == NoOfcol-1){
						preparedStmt.addBatch();
					}
				}	        
				long start = System.nanoTime();
				preparedStmt.execute();
//				con.commit();
				long end = System.nanoTime();
				System.out.println("Update (UploadRow) = " + (end-start)/ 1000000.0 + " ms");				
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
//				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (SQLException e){
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

		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		{	
			try
			{				
				String query = "insert into " + bucketName;
				String columns = " (rowId, ";
				String values = " values (?, ";
				String update = " ON DUPLICATE KEY UPDATE";
				String last = " rowId=values(rowId), ";
				for (int i = 0; i < opList.size(); i++){
					if (i == opList.size()-1){
						columns += "col" + i + ")";
						values += "?)";
						last += "col" + i + "=values(col" + i + ")";
					}
					else {
						columns += "col" + i + ", ";
						values += "?, ";
						last += "col" + i + "=values(col" + i + "), ";
					}			
				}
				
				String wholeQuery = query+columns+values+update+last;
				PreparedStatement preparedStmt = (PreparedStatement) con.prepareStatement(wholeQuery);
				
				preparedStmt.setInt(1, rowId);
				for (int i = 0; i < opList.size(); i++){
					op = opList.get(i);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					colId = Integer.parseInt(parts[1]);				
					DataItem data = op.getDataItem();
					preparedStmt.setBinaryStream(colId+2,new ByteArrayInputStream(data.getData()),data.getData().length);
				}	        
				long start = System.nanoTime();
				preparedStmt.executeUpdate();
//				con.commit();
				long end = System.nanoTime();
				System.out.println("Update (UploadRow) = " + (end-start)/ 1000000.0 + " ms");				
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
			} 
			catch (SQLException e){
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
		String key = "";
		UploadOperation op;
		String[] parts;
		int rowId;
		op = opList.get(0);
		key = op.getKey();
		parts = key.split("#");
		int NoOfcol = Integer.parseInt(parts[1]);
		DataItem data = null;
		
		
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		{	
			try
			{				
				String query = "UPDATE " + bucketName + " SET ";
				for (int i = 0; i < NoOfcol; i++){
					if (i == NoOfcol-1){
						query += "col" + i + " = ? WHERE rowId = ?";
					}
					else {
						query += "col" + i + " = ?, ";
					}			
				}
							
				PreparedStatement preparedStmt = (PreparedStatement) con.prepareStatement(query);
						
				for (int i = 0; i < opList.size(); i++){
					op = opList.get(i);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					rowId = Integer.parseInt(parts[0]);
			
					data = op.getDataItem();
				
					if(i%NoOfcol == 0){
						preparedStmt.setInt(NoOfcol+1, rowId);
					}
					
					preparedStmt.setBinaryStream(i%NoOfcol+1,new ByteArrayInputStream(data.getData()),data.getData().length);
					
					if(i%NoOfcol == NoOfcol-1){
						preparedStmt.addBatch();
					}
				}	        
				long start = System.nanoTime();
				preparedStmt.executeBatch();
//				con.commit();
				long end = System.nanoTime();
				System.out.println("Update (UploadRow) = " + (end-start)/ 1000000.0 + " ms");				
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
//				throw new RuntimeException("unable to connect to MongoDB " + mongoColl.getFullName(), e);
			} 
			catch (SQLException e){
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
		String key = "";
		UploadOperation op;
		String[] parts;
		key = opList.get(0).getKey();
		parts = key.split("#");
		int NoOfrow = Integer.parseInt(parts[1]);
		int colId;
		long end = System.nanoTime();
		System.out.println("Initial Time (UploadColPath) = " + (end-start)/ 1000000.0 + " ms");
		
		int NoOfcol = opList.size()/NoOfrow;
		ArrayList<String> colIds = new ArrayList<String>();
		
		for (int i = 0; i < NoOfcol; i++){
			op = opList.get(i*NoOfrow);
			key = op.getKey();
			parts = key.split("#");
			colIds.add(parts[0]);			
		}
		
		String query = "UPDATE " + bucketName + " SET ";
		for (int i = 0; i < NoOfcol; i++){
			if (i == NoOfcol-1){
				query += "col" + colIds.get(i) + " = ? WHERE rowId = ?";
			}
			else {
				query += "col" + colIds.get(i) + " = ?, ";
			}			
		}				
//		System.out.println(query);
			
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		{	
			try
			{	
				start = System.nanoTime();
				PreparedStatement preparedStmt = (PreparedStatement) con.prepareStatement(query);
				for (int i = 0; i < NoOfrow; i++){
					preparedStmt.setInt(NoOfcol+1, i);
					for (int k = 0; k < NoOfcol; k++){
						op = opList.get(i+k*NoOfrow);
						sop.add(new ScheduledOperation(op));
						key = op.getKey();
						parts = key.split("#");
						colId = Integer.parseInt(parts[0]);		
						DataItem data = op.getDataItem();
						preparedStmt.setBinaryStream(1+k,new ByteArrayInputStream(data.getData()),data.getData().length);
					}
					preparedStmt.addBatch();						
				}	  
				end = System.nanoTime();
				System.out.println("Middle Step (UploadColPath) = " + (end-start)/ 1000000.0 + " ms");
		
				long start2 = System.nanoTime();
				preparedStmt.executeBatch();
//				con.commit();
				long end2 = System.nanoTime();
				System.out.println("Update (UploadColPath) = " + (end2-start2)/ 1000000.0 + " ms");	
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
			} 
			catch (SQLException e){
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
				String query = "DELETE FROM " + bucketName + " WHERE rowId=" + key;
				st.executeQuery(query);

				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
			} 
			catch (SQLException e) {
				e.printStackTrace();
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
		
		String[] parts = srcKey.split("#");
		int nodeId = Integer.parseInt(parts[1]);
		
		parts = destKey.split("#");
		int destId = Integer.parseInt(parts[1]);
		
		ScheduledOperation sop = new ScheduledOperation(op);
		{	
			try
			{
				String query = "select data from " + bucketName + " where rowId=" + nodeId;
				ResultSet rs = st.executeQuery(query);
				String dataStr = "";
				
				while (rs.next()) {
					dataStr = rs.getString("data");
				}
						
				query = " insert into " + bucketName + " (rowId, data)"
				        + " values (?, ?)";
				PreparedStatement preparedStmt = (PreparedStatement) con.prepareStatement(query);
				preparedStmt.setInt(1, destId);
				preparedStmt.setString(2, dataStr);
				preparedStmt.execute();
					
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				sop.onFailure();
			} 
			catch (SQLException e) {
				e.printStackTrace();
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
			String query = "select rowId from " + bucketName;
			ResultSet rs = st.executeQuery(query);
			
			while (rs.next()) {
				ret.add(rs.getString("rowId"));
			}
			
			sop.onSuccess(new WrappedListDataItem(ret));
		}
		catch (NullPointerException e) { 
			sop.onFailure();
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}			
		return sop;
	}

	@Override
	public void disconnect() { 
		try {
			con.close();
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} 		
	}
	
	@Override
	public long totalByteSize()
	{
		long ret = 0;
		
		String query = "SELECT "
				+ "table_name AS `Table`,"
				+ "round(((data_length + index_length) ), 2) "
				+ "FROM information_schema.TABLES "
				+ "WHERE table_schema = \"remote\""
						+ "AND table_name = \"" + bucketName + "\"";
		
		ResultSet rs;
		try {
			rs = st.executeQuery(query);
			while (rs.next()) {
				ret = (long) Double.parseDouble(rs.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		

		return ret;
	}

	@Override
	public void cloneTo(String to) 
	{
		
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
			String query = "select data from " + bucketName + " where rowId in (" + path.toString().substring(1,path.toString().length()-1) + ")";
			ResultSet rs = st.executeQuery(query);

			int i = 0;
			try {
				while(rs.next()) {
					byte[] data = (rs.getBytes("data"));
//							Base64.getDecoder().decode(rs.getString("data"));
					sop.get(i).onSuccess(new SimpleDataItem(data));
					i++;
				}	
			} finally {
				rs.close();
			}
			return sop;
		}
		catch (NullPointerException e) { 
			for (int i = 0; i < opList.size(); i++)
				sop.get(i).onFailure();
		} 
		catch (SQLException e){
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
	public ArrayList<ScheduledOperation> uploadPath(ArrayList<UploadOperation> opList) {
		
		String key = "";
		UploadOperation op;
		String[] parts;
		int nodeId;
	
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		{	
			try
			{	
				String query = "insert into " + bucketName + " (rowId, data)"
				        + " values (?, ?)"
				        + " ON DUPLICATE KEY UPDATE"
				        + " rowId=values(rowId), data=values(data)";
				PreparedStatement preparedStmt = (PreparedStatement) con.prepareStatement(query);
				
				for (int i = 0; i < opList.size(); i++){
					op = opList.get(i);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					nodeId = Integer.parseInt(parts[1]);				
					DataItem data = op.getDataItem();
									
					preparedStmt.setInt(1, nodeId);
					preparedStmt.setBinaryStream(2,new ByteArrayInputStream(data.getData()),data.getData().length);
//					preparedStmt.setBytes(2, data.getData());
					preparedStmt.addBatch();
				}	
				preparedStmt.executeBatch();
//				con.commit();
				           
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onFailure();
			} 
			catch (SQLException e){
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
	public ArrayList<ScheduledOperation> downloadRow(ArrayList<DownloadOperation> op) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArrayList<ScheduledOperation> downloadCol(ArrayList<DownloadOperation> op) {
		// TODO Auto-generated method stub
		return null;
	}

	



}
