import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author xchen135
 *
 */
public class BoltFilterWorkerThread extends Thread
{
	public static Logger _logger = Logger.getLogger(BoltFilterWorkerThread.class);
	private String receiverId;
	private int port;
	private HashMap<String, Integer> wordCountMap = new HashMap<String, Integer>();
	
	public BoltFilterWorkerThread(int port, String id)
	{
		this.receiverId = id;
		this.port = port;
		
		wordCountMap.put("Facebook", 0);
		wordCountMap.put("Google", 0);
		wordCountMap.put("Twitter", 0);
		wordCountMap.put("Amazon", 0);
		wordCountMap.put("Apple", 0);
	}
	
	public void run()
	{
		_logger.info("BoltFilterWorkerThread start to send the filter result to Aggregator: "+receiverId+" !!");
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
						//TODO do the filter job here 
						String res = FilterApplication(streamline);					
						out.println(res);
						_logger.info("!!sending stream to "+receiverId+ " with message: "+streamline);
					}
				}
				
				// TODO we should send an end message to the aggregate bolt notify it the streaming is over. 
				out.println(Node._streammingStopMsg);
				
				System.out.println("Filter bolt stop sending out stream and stop message has been send out to "+receiverId);
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
	
	
	// the application logic here
	public String FilterApplication (String s )
	{
				
		String words[] = s.split("\\s");
		//TODO just for the fast testing, need to remove this later 
		// clean up the map before we wanna use it
		for(Entry<String, Integer> entry : wordCountMap.entrySet())
		{
			wordCountMap.put(entry.getKey(), 0);
		}
		
		for(String eachWord : words)
		{
			Integer count = 0;
			if(wordCountMap.containsKey(eachWord))
			{
				count = wordCountMap.get(eachWord);
				wordCountMap.put(eachWord, ++count);
			}
		}
		
		
		StringBuffer sb = new StringBuffer();
		for(Entry<String, Integer> entry : wordCountMap.entrySet())
		{
			String word = entry.getKey().trim();
			String count = entry.getValue().toString().trim();
			sb.append(word).append(Node._wcDel).append(count).append(Node._sDel);
		}
		
		return sb.toString();
	}
	
}
