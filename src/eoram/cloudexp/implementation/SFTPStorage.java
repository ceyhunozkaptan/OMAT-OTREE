package eoram.cloudexp.implementation;

import eoram.cloudexp.artifacts.*;
import eoram.cloudexp.data.*;
import eoram.cloudexp.interfaces.*;
import eoram.cloudexp.service.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;


/**
 * Implements a storage interface for MongoDB.
 */
public class SFTPStorage implements InternalStorageInterface 
{	
	String sftpHost = "ec2-54-202-57-99.us-west-2.compute.amazonaws.com";
    int sftpPort = 22;
    String sftpUser = "ubuntu";
    String sftpPkey = "/nfs/stak/students/o/ozkaptac/Desktop/amazon_pro_oregon.pem";
//    String SFTPPASS = "password";
    String sftpDir = "remote";

    Session sshSession = null;
    Channel channel = null;
    ChannelSftp channelSftp = null;
	
	private String bucketName = null;
	
	private boolean resetBucket = false;
	
	public SFTPStorage(boolean shouldReset) { resetBucket = shouldReset; }

	@Override
	public void connect() 
	{
		
        try {
        	JSch jsch = new JSch();
    		
    		sshSession = jsch.getSession(sftpUser, sftpHost, sftpPort);
    		jsch.addIdentity(sftpPkey);
    		
    		java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            sshSession.setConfig(config);
			sshSession.connect();
			channel = sshSession.openChannel("sftp");
			channel.connect();
			channelSftp = (ChannelSftp) channel;
			
			String storagekey = SessionState.getInstance().storageKey.toLowerCase();
			String[] parts = storagekey.split("--");	
			bucketName = parts[1];
			
			channelSftp.cd(sftpDir);
			channelSftp.mkdir(bucketName);
			channelSftp.cd(bucketName);
			
		} catch (JSchException e) {
			System.out.println(e);
		} catch (SftpException e) {
			System.out.println(e);
		}
		   
		
//		if (resetBucket)
//			mongoColl.drop();
	}

	@Override
	public ScheduledOperation downloadObject(DownloadOperation op) 
	{
			
		ScheduledOperation sop = new ScheduledOperation(op);
		
		return sop;
	}
	
	@Override
	public ScheduledOperation downloadAll(DownloadBulkOperation op) 
	{
				
		ScheduledOperation sop = new ScheduledOperation(op);
		
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> downloadPath(ArrayList<DownloadOperation> opList) 
	{
		String key = "";
		DownloadOperation op;
		String[] parts;
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		
		List<String> path = new ArrayList<String>();
		for (int i = 0; i < opList.size(); i++){
			op = opList.get(i);
			sop.add(new ScheduledOperation(op));
			key = op.getKey();
			parts = key.split("#");
			path.add(parts[1]);	
		}
		
		try
		{	

			for (int i = 0; i < path.size(); i++){		
				InputStream is = channelSftp.get(path.get(i));
				
				byte[] data = IOUtils.toByteArray(is);
	
				sop.get(i).onSuccess(new SimpleDataItem(data));
			}

			return sop;
		}
		catch (NullPointerException | IOException | SftpException e) { 
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
		
		ScheduledOperation sop = new ScheduledOperation(op);
		
		return sop;
	}
	
	// FIXME
	@Override
	public ArrayList<ScheduledOperation> downloadRow(ArrayList<DownloadOperation> opList) 
	{
		
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		
		return sop;
	}
	
	@Override
	public ScheduledOperation downloadCol(DownloadBulkOperation op) 
	{
		
		ScheduledOperation sop = new ScheduledOperation(op);
		
		return sop;
	}
	
	@Override
	public ScheduledOperation uploadObject(UploadOperation op) 
	{
		
		ScheduledOperation sop = new ScheduledOperation(op);
		
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadPath(ArrayList<UploadOperation> opList) 
	{
		String key = "";
		UploadOperation op;
		String[] parts;
		int nodeId;
		ByteArrayInputStream bais = null;
		
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

					bais = new ByteArrayInputStream(data.getData());
					channelSftp.put(bais, nodeId+"");	
					
				}	        

				bais.close();
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				return sop;
			} catch (SftpException | IOException e) {
				System.out.println(e);
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
		String key = "";
		UploadOperation op;
		String[] parts;
		String nodeId;
		DataItem data = null;
		
		ByteArrayInputStream bais = null;
		
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		{
	
			try {
				for (int i = 0; i < opList.size(); i++){
					op = opList.get(i);
					opList.set(i, null);
					sop.add(new ScheduledOperation(op));
					key = op.getKey();
					parts = key.split("#");
					nodeId = parts[1];				
					data = op.getDataItem();		
					op = null;
					bais = new ByteArrayInputStream(data.getData());
					channelSftp.put(bais, nodeId);	
					
				}	
				
				bais.close();
				
				for (int i = 0; i < opList.size(); i++)
					sop.get(i).onSuccess(new EmptyDataItem());
				opList = null;
				return sop;
				
			} catch (SftpException | IOException  e) {
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
		
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();

		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadAllRow(ArrayList<UploadOperation> opList)
	{	
		
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();

		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadRowPath(ArrayList<UploadOperation> opList)
	{	
		
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		
		return sop;
	}
	
	@Override
	public ArrayList<ScheduledOperation> uploadColPath(ArrayList<UploadOperation> opList) 
	{
		
		ArrayList<ScheduledOperation> sop = new ArrayList<ScheduledOperation>();
		
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
				

				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (NullPointerException e) { 
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
	
		return sop;
	}
	
	@Override
	public ScheduledOperation listObjects(ListOperation op) 
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		
		return sop;
	}

	@Override
	public void disconnect() { 
		channelSftp.exit();
        channel.disconnect();
        sshSession.disconnect();

	}
	
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
