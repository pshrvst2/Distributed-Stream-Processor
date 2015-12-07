import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author xchen135
 *
 */
public class BoltAggregateWorkerThread extends Thread
{
	public static Logger _logger = Logger.getLogger(BoltAggregateWorkerThread.class);
	private String introducerIp;
	private int port; 
	
	
	
	public BoltAggregateWorkerThread(int port, String ip)
	{
		this.introducerIp = ip;
		this.port = port;
		//TODO handle this on the config file
		/*Node._resultMap.put("Facebook", 0);
		Node._resultMap.put("Google", 0);
		Node._resultMap.put("Twitter", 0);
		Node._resultMap.put("Amazon", 0);
		Node._resultMap.put("Apple", 0);*/
		
		
	}
	
	public void run()
	{
		_logger.info("BoltAggregateWorkerThread start to work on aggregation and send result to introducer: "+introducerIp+" !!");
		if(!introducerIp.isEmpty())
		{
			try
			{	
				String serverhost = introducerIp;
				Socket socket = new Socket(serverhost, port);
				BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				String streamline ="";
				
				while(!Node._streamReadingStop && !Node._faultToleranceStop)
				{
					if((streamline= Node._streamingList.poll()) != null)
					{
						//System.out.println("In sink now and try to process the stream: "+ streamline);
						//TODO the aggregate job here 
						aggregatorApplication(streamline);	
		
						_logger.info("!!sending stream to "+introducerIp+ " with message: "+streamline);
					}
				}
				
				//TODO use a for loop here to print out the result message. 
				if(Node._faultToleranceStop)
				{
					out.println(" Fault detected!! Force to stop and drop all the work!! ");
				}
				else
				{
					for (HashMap.Entry<String, Integer> record : Node._resultMap.entrySet())
					{
						out.println("||| Key word : "+ record.getKey() + " | Counts: " + record.getValue()+" |||");
					}
				}						
				// send the job is done message to the introducer to notify it the job has been accomplished
				out.println(Node._jobIsCompletedMsg);
				Node._resultMap.clear();
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
		
	
	public void aggregatorApplication(String s)
	{

		String sentences[] = s.split(Node._sDel);
		
		for(String eachWCPair : sentences)
		{
			String wAndC[] = eachWCPair.split(Node._wcDel);
			String word = wAndC[0];
			String count = wAndC[1];
			if(Node._resultMap.containsKey(word))
			{
				int num =0;
				try
				{
					num =Integer.parseInt(count);
				}
				catch(Exception e)
				{
					_logger.error(e);
				}
				
				num += Node._resultMap.get(word);
				Node._resultMap.replace(word, num);
			}
			else
			{
				int num = Integer.parseInt(count);
				Node._resultMap.put(word, num);
			}
		}
	}
}
