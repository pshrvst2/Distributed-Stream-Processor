import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

import org.apache.log4j.Logger;


/*
 * This Thread should only be called by the introducer.
 * 
 */

public class CraneRoleSenderThread extends Thread{

	public static Logger _logger = Logger.getLogger(CraneRoleSenderThread.class);
	private int port;
	private String receiverId;
	private String message;
	
	
	public CraneRoleSenderThread(int port, String id, String msg)
	{
		this.port = port;
		this.receiverId = id;
		this.message = msg;
	}
	
	public void run()
	{
		_logger.info("CraneRoleSenderThread initialzing....");
		if(!receiverId.isEmpty() && !message.isEmpty())
		{
			try
			{
				_logger.info("Sending Role message to "+receiverId+ "with message: "+ message);
				
				String serverhost = receiverId.substring(0, receiverId.indexOf(":")).trim();
				Socket socket = new Socket(serverhost, port);
				BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				out.println(message);
				
				String returnStr = "";
				
				//Introducer should only update the listing value when it is sending out the role message
				// if it's sending out reset message, introducer should not set them listening value as true
				if(message.contains(Node._craneRoleMessage))
				{
					while ((returnStr = in.readLine()) != null) 
					{
						_logger.info(" Received from "+receiverId+" message: " + returnStr);
						if(returnStr.equals(Node._craneBoltListenningMsg))
						{
							Node._gossipMap.get(receiverId).setListening(true);
						}
					}
				}
				
				
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
