import java.net.Inet4Address;
import java.net.ServerSocket;

/**
 * 
 */

/**
 * @author xchen135
 *
 */
public class RoleListener extends Thread {
	
	private ServerSocket serverSocketListener;

	/*public void RoleListener()
	{
		
	}*/

	public void run() {

		try 
		{	
			serverSocketListener = new ServerSocket(Node._TCPPortForCraneRole);
			String serverIp = Inet4Address.getLocalHost().getHostAddress();
			while (!Node._craneRoleListenerThreadStop) 
			{
				new CraneRoleListenerThread(serverSocketListener.accept(), serverIp).start();
				//write logs
			}
		}
		catch(Exception e)
		{
			
		}
	}
}
