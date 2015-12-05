import java.net.Inet4Address;
import java.net.ServerSocket;

import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author xchen135
 *
 */
public class BoltListener extends Thread 
{
	private ServerSocket serverSocketListener;
	public static Logger _logger = Logger.getLogger(BoltListener.class);

	public void run() {

		try 
		{	
			serverSocketListener = new ServerSocket(Node._TCPPortForStreaming);
			String serverIp = Inet4Address.getLocalHost().getHostAddress();
			while (!Node._craneRoleListenerThreadStop) 
			{
				
				new BoltListenerThread(serverSocketListener.accept(), serverIp).start();
				_logger.info("CraneRoleListenerThread Initialing .... ");
				
			}
		}
		catch(Exception e)
		{
			_logger.error(e);
		}
	}
}
