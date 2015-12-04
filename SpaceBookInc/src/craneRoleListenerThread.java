import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;


public class craneRoleListenerThread extends Thread
{
	public static Logger _logger = Logger.getLogger(craneRoleListenerThread.class);
	private int port;
	
	public craneRoleListenerThread(int port) 
	{
		this.port = port;
	}
	
	public void run()
	{
		_logger.info("craneRoleListenerThread initialzing....");
		ServerSocket listener = null;
		try 
		{
			listener = new ServerSocket(port);
			
            while (!Node._craneRoleListenerThreadStop) 
            {
            	Socket socket = listener.accept();
            	BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String message = in.readLine();
				_logger.info("_craneRoleListenerThreadStop received message : "+message);
				if(message == null || message.isEmpty())
				{
					//nothing 
				}
				else if(message.contains(Node._craneRoleMessage))
				{
					String role = message.substring(message.indexOf("[")+1, message.indexOf("]"));
					Node._gossipMap.get(Node._machineId).setType(role);
					//TODO start the bolt function file listener here 
					sendRoleUpdateMsg(Node._introducerIp);
					Node._gossipMap.get(Node._machineId).setListening(true);
				}
				else if(message.contains(Node._craneBoltListenningMsg))
				{
					String id = message.substring(message.indexOf("[")+1, message.indexOf("]"));
					Node._gossipMap.get(id).setListening(true);
				}
            }
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
        finally 
        {
				try 
				{
					listener.close();
				} 
				catch (IOException e) 
				{
					_logger.error(e.getMessage());
			
				}
        }
	}
	
	
	
	public void sendRoleUpdateMsg(String ip) throws UnknownHostException, IOException
	{
		_logger.info("Sending OK _craneBoltListenningMsg from: "+Node._machineId+" to "+ ip);
		Socket socket = new Socket(ip, Node._TCPPortForCraneRole);
		BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
		PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
		out.println(Node._craneBoltListenningMsg + "["+Node._machineId+"]");
		
		String servermsg ="";
		while((servermsg = in.readLine()) !=null)
		{
			// not sure wether we should use this! 
			_logger.info(" sendRoleUpdateMsg receiving "+servermsg);
		}
		
		out.close();
		in.close();
		socket.close();
	}
}
