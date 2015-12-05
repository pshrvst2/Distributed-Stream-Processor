import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author xchen135
 *
 */
public class SpoutWorkerThread extends Thread 
{
	public static Logger _logger = Logger.getLogger(SpoutWorkerThread.class);
	private String receiverId;
	private int port; 
	
	public SpoutWorkerThread(int port, String id)
	{
		this.receiverId = id;
		this.port = port;
	}
	
	public void run()
	{
		_logger.info("SpoutWorkerThread for the filter bolt: "+receiverId+" initialzing....");
		if(!receiverId.isEmpty())
		{
			try
			{	
				String serverhost = receiverId.substring(0, receiverId.indexOf(":")).trim();
				Socket socket = new Socket(serverhost, port);
				BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				String streamline ="";
				
				while(!Node._streamReadingStop)
				{
					if((streamline= Node._streamingList.poll()) != null)
					{
						out.println(streamline);
						_logger.info("!!sending stream to "+receiverId+ " with message: "+streamline);
					}
				}
				
				// TODO we should send an end message to the filter bolt notify it the streaming is over. 
				out.println(Node._spoutStreammingStopMsg);
				// TODO we should wait for the crane job done message from bolt-sink to notify the spout that the whole process has been accomplished 
				/*
				while ((returnStr = in.readLine()) != null) 
				{
					if(returnStr == jobDoneMessage )
					{
						break;
					}
				}
				*/
				out.close();
				in.close();
				socket.close();		
			}
			catch (SocketException e) 
			{
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
			catch(IOException ioExcep)
			{
				_logger.error(ioExcep);
				//ioExcep.printStackTrace();
			}
		}
	}
}
