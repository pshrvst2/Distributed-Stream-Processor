import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author xchen135
 *
 */
public class JobDoneListenerThread extends Thread
{
	public static Logger _logger = Logger.getLogger(JobDoneListenerThread.class);
	private int port;
	private String ipAddr = null;
	private Socket clientSocket = null;

	public JobDoneListenerThread(Socket socket, String ip) 
	{
		this.clientSocket = socket;
		this.ipAddr = ip;
	}

	public void run()
	{
		_logger.info("JobDoneListenerThread initialzing....");
		try 
		{
			boolean resultTitleDisplay = false;
			String message = "";
			BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			while((message = reader.readLine())!=null )
			{
				// receive stream message from spout. stop the worker for writing the stream into the concurrent list
				
				if(message.equals(Node._jobIsCompletedMsg))
				{
					break;
				}
				else
				{
					if(resultTitleDisplay)
					{
						System.out.println(message);
					}
					else
					{
						System.out.println("The job has been accomplished, the result as following: ");
						System.out.println(message);
						resultTitleDisplay = true;
					}
				}
			}
			reader.close();
			clientSocket.close();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
	}

}
