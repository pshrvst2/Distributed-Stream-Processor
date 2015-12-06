import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;

import org.apache.log4j.Logger;


public class CraneRoleListenerThread extends Thread
{
	public static Logger _logger = Logger.getLogger(CraneRoleListenerThread.class);
	private int port;
	private String ipAddr = null;
	private Socket clientSocket = null;

	public CraneRoleListenerThread(Socket socket, String ip) 
	{
		this.clientSocket = socket;
		this.ipAddr = ip;
	}

	public void run()
	{
		_logger.info("craneRoleListenerThread initialzing....");
		try 
		{

			String message = "";
			BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(),true);

			message = reader.readLine();
			_logger.info("_craneRoleListenerThreadStop received message : "+message);
			
			if(message == null || message.isEmpty())
			{
				//nothing 
			}
			else if(message.contains(Node._craneRoleMessage))
			{
				String aggrId = message.substring(message.indexOf("[")+1, message.indexOf("]"));
				
				UpdateCraneRoleforLocal(aggrId);
				
				if(Node._machineId.equalsIgnoreCase(aggrId))
				{
					
					
					// start to listen to the filter bolts
					Thread BoltListener = new BoltListener();
					BoltListener.start();
					
					// need wait for the listening server up before we try to start a tcp connection
					try {
			            Thread.sleep(1000);
			        }
			        catch (InterruptedException ie) {
			            // Handle the exception
			        }
					
					// start the aggregator listener here
					// activateAggregatorWorkers
					// Use introducer's id, since we need to return the result to the introducer. 
					Thread BoltAggregateWorkerThread = new BoltAggregateWorkerThread(Node._TCPPortForJobReport,Node._introducerIp);
					BoltAggregateWorkerThread.start();
						
				}
				else
				{
					
					// start to listen to the spout
					Thread BoltListener = new BoltListener();
					BoltListener.start();
					
					// need wait for the listening server up before we try to start a tcp connection
					try {
			            Thread.sleep(1000);
			        }
			        catch (InterruptedException ie) {
			            // Handle the exception
			        }
					
					// start the filter listener here 
					// activateFilterWorkers
					Thread BoltFilterWorkerThread = new BoltFilterWorkerThread(Node._TCPPortForStreaming, aggrId);
					BoltFilterWorkerThread.start();
					
					
					
				}
				pw.println(Node._craneBoltListenningMsg);
			}
			else if(message.contains(Node._craneRoleResetMessage))
			{
				ResetCraneRoleforLocal();
			}
			
			pw.close();
			reader.close();
			clientSocket.close();

		
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
	}

	public void UpdateCraneRoleforLocal(String aggrNum)
	{
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
		{
			NodeData temp = record.getValue();
			if(record.getKey().equalsIgnoreCase(aggrNum))
			{
				temp.setType(Node._bolt_aggregate);
			}
			else if(!record.getKey().substring(0, record.getKey().indexOf(":")).equals(Node._introducerIp))
			{
				temp.setType(Node._bolt_filter);
			}
			temp.setListening(true);
		}
	}
	
	public void ResetCraneRoleforLocal()
	{
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
		{
			// set the crane role as None for all the memeber except for the introducer
			if(!record.getKey().contains(Node._introducerIp))
			{
				record.getValue().setType("None");
				record.getValue().setListening(false);
			}
		}
	}

}
