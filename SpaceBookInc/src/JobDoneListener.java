import java.net.Inet4Address;
import java.net.ServerSocket;

import org.apache.log4j.Logger;

/**
 * This listener only work for the introducer that he can listen to the aggregate bolt when all the job has been done!!
 */

/**
 * @author xchen135
 *
 */
public class JobDoneListener extends Thread
{
	private ServerSocket serverSocketListener;
	public static Logger _logger = Logger.getLogger(JobDoneListener.class);

	public void run() {

		try 
		{	
			serverSocketListener = new ServerSocket(Node._TCPPortForJobReport);
			String serverIp = Inet4Address.getLocalHost().getHostAddress();
			while (!Node._jobIsCompleted) 
			{
				new JobDoneListenerThread(serverSocketListener.accept(), serverIp).start();
				_logger.info("JobDoneListener Initialing .... ");
			}
		}
		catch(Exception e)
		{
			_logger.error(e);
		}
	}
}
