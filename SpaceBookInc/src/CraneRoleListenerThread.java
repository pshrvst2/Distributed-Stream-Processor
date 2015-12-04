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
		this.setIpAddr(ip);
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
				if(Node._machineId.equalsIgnoreCase(aggrId))
				{
					// start the aggregator listener here
				}
				else
				{
					// start the filter listener here 
				}
				UpdateCraneRoleforLocal(aggrId);
				pw.println(Node._craneBoltListenningMsg);
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

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getIpAddr() {
		return ipAddr;
	}

	public void setIpAddr(String ipAddr) {
		this.ipAddr = ipAddr;
	}




	/*

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
	}*/
}
