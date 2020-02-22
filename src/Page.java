import java.io.*;
import java.util.*;


public class Page implements Serializable {
	private Vector<Tuple> tuples = new Vector();
	private transient String pageName;
	private transient int lastIn;
    public void addTuple(Tuple t) {
    	tuples.add(t);
    }

	public Vector<Tuple> getTuples() {
		return tuples;
	}

	public void setTuples(Vector<Tuple> tuples) {
		this.tuples = tuples;
	}

	public String getPageName() {
		return pageName;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

	public int getLastIn() {
		return lastIn;
	}

	public void setLastIn(int lastIn) {
		this.lastIn = lastIn;
	}

	public static void main(String[] args) throws Exception{
		Page h = new Page();
		Tuple abdo = new Tuple();
		// Vector g = new Vector();
		abdo.addAttribute("mphsen");
		abdo.addAttribute(2001);
		// abdo.z.add(true);

		Tuple abdo2 = new Tuple();
		// Vector l = new Vector();
		abdo2.addAttribute("shabra");
		abdo2.addAttribute(1999);
		// abdo2.z.add(true);

		h.tuples.add(abdo);
		h.tuples.add(abdo2);
		System.out.println(h.tuples);

		try {
			FileOutputStream fileOut = new FileOutputStream("childSer3.ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(h.tuples);
			out.close();
			fileOut.close();
			System.out.println("Serialized data is done");

			FileInputStream fileIn = new FileInputStream("childSer3.ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Vector xx = (Vector) in.readObject();
			in.close();
			fileIn.close();
			System.out.println(xx);

		} catch (Exception e) {
			e.printStackTrace();

		}

	}
	public String toString()
	{
		StringBuilder sb=new StringBuilder();
		for(Object o:tuples) {
			Tuple x=(Tuple)o;
			sb.append(x.toString());
			sb.append("#");
			
		}
		return sb.toString();
	}
}
