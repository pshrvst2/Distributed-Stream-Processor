import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;


/**
 * @author xchen135
 *
 */
public class SpoutStarter 
{
	private String ip;
	public static Logger _logger = Logger.getLogger(SpoutStarter.class);
	
	public SpoutStarter(String ip)
	{
		this.ip =ip;
	}
	
	public void start()
	{
		if(ip.equals(Node._introducerIp))
		{
			if(!isReady())
			{
				System.out.println("Crane is not ready, please try to assign Crane role first ");
			}
			else
			{
				// start the thread to send out the buffer here before the read stream
				// make sure it won't send out anything if the stream is empty
				activateSpoutWorkers();						
				readStream();
				// start the timer to log the start time
				Node._startTime = new Date().getTime();
				
				// check whether the stream list elements have all been distributed by the sport worker 				
				while (Node._streamingList.size()!=0 && !Node._faultToleranceStop)
				{
					//_logger.info("There are still "+Node._streamingList.size()+" has been waiting for distrubted....");
				}
				
				Node._streamReadingStop = true;
				
				//need a gate keeper here to clean up the concurrent list in case we use the _faultToleranceStop flag to shut down the system
				Node._streamingList.clear();
				
				
				System.out.println("Stream concurrent list is empty now, all the message should have been sent out ");
			}
			
		}
		else
		{
			System.out.println("You don't have the previlige to start the spout! ");
		}
	}
	
	/*
	 * Method that checks if all the blot assigned a role and start to listen to the sport
	 */
	public boolean isReady()
	{
		boolean flag = true;
		
		if(Node._gossipMap.size()<2)
		{
			flag =false;
		}
		else
		{
			for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
			{
				NodeData temp = record.getValue();
				// put the all the ids excpet for the introducers
				if(temp.getType().equals("None"))
				{
					flag = false;
					break;
				}
				else if(!temp.isListening())
				{
					flag = false;
					break;
				}
			}
		}
		return flag;
	}
	
	/*
	 * method that tries to read the stream for the spout
	 */
	public void readStream()
	{
		try 
		{
			if(Node._gossipMap.get(Node._machineId).getApplicationNum() == 0)
			{
				// This message should never display, put here just for debug
				System.out.println( "No application has been selected!!" );
			}
			else
			{
				String fileName ="";
				if(Node._gossipMap.get(Node._machineId).getApplicationNum() ==1)
				{
					fileName = Node._1_FilterAggrFileName;
				}
				else if(Node._gossipMap.get(Node._machineId).getApplicationNum() ==2)
				{
					fileName = Node._2_WordCountFileName;
				}					
				FileReader fileReader = new FileReader(Node._streamFilePath+fileName);
				BufferedReader bufReader = new BufferedReader(fileReader);
				String line = null;
				while((line = bufReader.readLine()) != null && !Node._faultToleranceStop)
				{
					Node._streamingList.add(line);	
				}
				bufReader.close();
				
			}			
		} 
		catch (FileNotFoundException e) 
		{
			_logger.error(e);
		}
		catch( IOException ex)
		{
			_logger.error(ex);
		}
		
	}
	
	/*
	 * Method that activates number of spout worker nodes 
	 */
	public void activateSpoutWorkers()
	{
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
		{
			NodeData temp = record.getValue();
			if(temp.getType().equalsIgnoreCase(Node._bolt_filter))
			{
				//Thread sportWorkerThread = SpoutWorkerThread(Node.);
				Thread sportWorkerThread = new SpoutWorkerThread(Node._TCPPortForStreaming, record.getKey());
				sportWorkerThread.start();
			}
		}
	}
	
}
