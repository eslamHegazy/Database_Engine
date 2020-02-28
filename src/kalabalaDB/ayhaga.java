package kalabalaDB;
import java.util.Vector;

public class ayhaga {
	public static void main(String[]args)
	{
		String x = "pagename" + "#" + "1";
		String[] arrOfStr = x.split("#");
		String y =arrOfStr[0];
		int z = Integer.parseInt(arrOfStr[1]);
		//System.out.println(z);
		//Page page1 = new Page();
		//Page page2 = new Page();
		Vector <String> xx = new Vector();
		xx.add("page1");
		xx.add("page2");
		for(int i = 0 ; i < xx.size();i++)
		{
			if(xx.get(i).equals("page1"))
			{
				System.out.println("true");
			}
		}
		
		int pos = 4; 
		int val = 4;
		Vector<Integer> yy = new Vector();
		yy.add(1);
		yy.add(2);
		yy.add(3);
		yy.add(3);		
		yy.add(4);
		yy.add(4);
		yy.add(4);
		yy.add(4);
		yy.add(6);
		yy.add(4);
		for(int i = pos ; i < yy.size();i++)
		{
			if(yy.get(i).equals(val))
			{
				yy.remove(i);
				i--;
			}
		}
		System.out.println(yy);
		System.out.println(yy.remove(4));
		System.out.println(yy);
		yy.setElementAt(67, 0);
		System.out.println(yy);
	}
}
