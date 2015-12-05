import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;

import org.apache.log4j.Logger;

/**
 * class use to update its own concurrentList 
 */

/**
 * @author xchen135
 *
 */
public class BoltListenerThread extends Thread
{

	public static Logger _logger = Logger.getLogger(BoltListenerThread.class);
	private String ipAddr = null;
	private Socket clientSocket = null;

	public BoltListenerThread(Socket socket, String ip) 
	{
		this.clientSocket = socket;
		this.ipAddr= ip;
	}

	public void run()
	{
		_logger.info("craneRoleListenerThread initialzing....");
		try 
		{

			String message = "";
			BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(),true);	
			// TODO: may not need a while loop 
			while((message = reader.readLine())!=null )
			{
				// receive stream message from spout. stop the worker for writing the stream into the concurrent list
				if(message.equals(Node._streammingStopMsg))
				{
					break;
				}
				else
				{
					Node._streamingList.add(message);
					_logger.info("!!!!!!!!!!!!!!!!!!BoltListenerThread received message : "+message);
				}			
			}
			
			//turn off the filter sending thread or aggregate bolt worker, since the job are all done here!  
			while (Node._streamingList.size()!=0)
			{
				//_logger.info("There are still "+Node._streamingList.size()+" has been waiting for distrubted....");
			}
			Node._streamReadingStop = true;
			_logger.info("!!!!!!!!!!!!!!!!!!!!_streamReadingStop");
			// TODO do we need to update something here
			pw.close();
			reader.close();
			clientSocket.close();

		
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
	}


}
