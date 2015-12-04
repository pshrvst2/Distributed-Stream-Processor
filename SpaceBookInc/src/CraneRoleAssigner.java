import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;


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
			if(serverCounts >2)
			{
				ArrayList<String> blotIds = new ArrayList<String>();
				for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
				{
					NodeData temp = record.getValue();
					// put the all the ids excpet for the introducers
					if(temp.getType().equals("None"))
					{
						blotIds.add(record.getKey());
					}
				}
				// take out the introducer
				updateCraneRole(blotIds,serverCounts-1);				
			}
			else
			{
				System.out.println("There is no enough memeber on the list, please try again later");
			}
		}
		else
		{
			System.out.println("You don't have the previlige to assign crane role");
		}
	}
	
	
	public void updateCraneRole(ArrayList<String> ids, int num)
	{
		int aggrNum = getRandomNumInRange(num);
		String aggrId = ids.get(aggrNum);
		for(String id: ids)
		{
			CraneRoleThreadStarter(id,aggrId);
		}
		
	}
	
	public int getRandomNumInRange(int num)
	{		
		Random rand = new Random();
		return rand.nextInt(num);
	}
	
	public void CraneRoleThreadStarter(String id, String aggrId)
	{
		Thread CraneRoleSenderThread = new CraneRoleSenderThread(Node._TCPPortForCraneRole, id, Node._craneRoleMessage+"["+aggrId+"]");
		CraneRoleSenderThread.start();
	}
	
	
	
}
