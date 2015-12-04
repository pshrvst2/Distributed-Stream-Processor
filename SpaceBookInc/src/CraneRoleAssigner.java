import java.util.ArrayList;
import java.util.HashMap;


public class CraneRoleAssigner {

	private String ip;
	public CraneRoleAssigner(String machineIp)
	{
		this.ip = machineIp;
	}
	
	public void assignRole()
	{
		if(ip.equals(Node._introducerIp))
		{
			int serverCounts = 0;
			serverCounts = Node._gossipMap.size();
			
			// TODO change this logic later, make it more generic 
			if(serverCounts >1)
			{
				ArrayList<String> blotIds = new ArrayList<String>();
				for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
				{
					NodeData temp = record.getValue();
					if(temp.getType().equals("None"))
					{
						blotIds.add(record.getKey());
					}
				}
				if(serverCounts == 3)
				{
					updateCraneRoleFor3(blotIds);
				}
				else if(serverCounts ==4)
				{
					updateCraneRoleFor4(blotIds);
				}
				else if(serverCounts ==6)
				{
					updateCraneRoleFor6(blotIds);
				}
				else
				{
					System.out.println("The number of vm is not support at this verison");
				}
			}
			else
			{
				System.out.println("There is no other memeber on the list, please try again later");
			}
		}
		else
		{
			System.out.println("You don't have the previlige to assign crane role");
		}
	}
	public void updateCraneRoleFor3 (ArrayList<String> ids)
	{
		Node._gossipMap.get(ids.get(0)).setType(Node._bolt_filter);
		CraneRoleThreadStarter(ids.get(0),Node._bolt_filter);
		
		Node._gossipMap.get(ids.get(1)).setType(Node._bolt_aggregate_sink);
		CraneRoleThreadStarter(ids.get(1),Node._bolt_aggregate_sink);
	}
	
	public void updateCraneRoleFor4 (ArrayList<String> ids)
	{
		Node._gossipMap.get(ids.get(0)).setType(Node._bolt_filter);
		CraneRoleThreadStarter(ids.get(0),Node._bolt_filter);
		
		Node._gossipMap.get(ids.get(1)).setType(Node._bolt_aggregate_sink);
		CraneRoleThreadStarter(ids.get(1),Node._bolt_aggregate_sink);
		
		Node._gossipMap.get(ids.get(2)).setType(Node._bolt_filter);
		CraneRoleThreadStarter(ids.get(2),Node._bolt_filter);
	}
	
	public void updateCraneRoleFor6 (ArrayList<String> ids)
	{
		Node._gossipMap.get(ids.get(0)).setType(Node._bolt_filter);
		CraneRoleThreadStarter(ids.get(0),Node._bolt_filter);
		
		Node._gossipMap.get(ids.get(1)).setType(Node._bolt_aggregate_sink);
		CraneRoleThreadStarter(ids.get(1),Node._bolt_aggregate_sink);
		
		Node._gossipMap.get(ids.get(2)).setType(Node._bolt_filter);
		CraneRoleThreadStarter(ids.get(2),Node._bolt_filter);
		
		Node._gossipMap.get(ids.get(3)).setType(Node._bolt_filter);
		CraneRoleThreadStarter(ids.get(3),Node._bolt_filter);
		
		Node._gossipMap.get(ids.get(4)).setType(Node._bolt_aggregate);
		CraneRoleThreadStarter(ids.get(4),Node._bolt_aggregate);
	}
	
	public void CraneRoleThreadStarter(String id, String role)
	{
		Thread CraneRoleSenderThread = new CraneRoleSenderThread(Node._TCPPortForCraneRole, id, Node._craneRoleMessage+"["+role+"]");
		CraneRoleSenderThread.start();
	}
	
}
