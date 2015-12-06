import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;

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
						System.out.println("***********************************************************");
						System.out.println("The job has been accomplished, the result as following: ");
						System.out.println(message);
						resultTitleDisplay = true;
					}
				}
			}
			Node._finishTime =new Date().getTime();
			long diff = Node._finishTime - Node._startTime;
			System.out.println(" The total time used for the tasked is : "+ diff + " ms");
			System.out.println("***********************************************************");
			
			// call the reset crane role method to tell all the member to reset the role
			resetCraneRole();
			
			reader.close();
			clientSocket.close();
			Node._jobIsCompleted = true;
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
	}
	
	public static void resetCraneRole()
	{
		/* 
		 * clean up the role assignment when task has been done
		 * Need to re-assign the role if user wanna start a new task
		 * Using this desgin to in favor for the fault tolerance function
		 * TODO maybe have a better design in the future 
		*/
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
		{
			if(!record.getValue().getType().equals(Node._spout))
			{
				record.getValue().setType("None");
				record.getValue().setListening(false);
				
				// send out the reset role message to all the member to reset their role as None.
				Thread CraneRoleSenderThread = new CraneRoleSenderThread(Node._TCPPortForCraneRole, record.getKey(), Node._craneRoleResetMessage);
				CraneRoleSenderThread.start();
			}
		}
		System.out.println("*** NOTE: role has been clean up, please assign role before start a new job *** ");
		// send out the reset role message to all the member to reset their role as None.
	}

}
