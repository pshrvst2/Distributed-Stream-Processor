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
public class CraneRoleListener extends Thread {
	
	private ServerSocket serverSocketListener;
	public static Logger _logger = Logger.getLogger(CraneRoleListener.class);

	public void run() {

		try 
		{	
			serverSocketListener = new ServerSocket(Node._TCPPortForCraneRole);
			String serverIp = Inet4Address.getLocalHost().getHostAddress();
			while (!Node._craneRoleListenerThreadStop) 
			{
				new CraneRoleListenerThread(serverSocketListener.accept(), serverIp).start();
				_logger.info("CraneRoleListenerThread Initialing .... ");
			}
		}
		catch(Exception e)
		{
			_logger.error(e);
		}
	}
}
