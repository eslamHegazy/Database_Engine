package DBV2;
import java.io.*;
import java.util.*;


public class Page implements Serializable {
	private Vector<Tuple> tuples;
	private transient String pageName;
	private static transient int lastIn=0;
    public Page(){
    	tuples=new Vector<Tuple>();
    	pageName="page"+lastIn;
    	lastIn++;
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
	
	public int size(){
		return tuples.size();
	}
	public void insertIntoTVector(Tuple x,int m){
		for(int i=m;i<tuples.size()-1;i++){
			Tuple c=tuples.get(i);
			tuples.insertElementAt(x, i);
			x=c;
		}
		tuples.insertElementAt(x,tuples.size()-1);
	}
	public void insertIntoPage(Tuple x,int pos){
		String nKey=x.getAttributes().get(pos).toString();
		int lower=0;
		int upper=tuples.size();
		if(upper==0){
			tuples.addElement(x);
		}else{
			while(lower<upper){
				int m=(lower+upper)/2;
				if(nKey.compareTo(tuples.get(m).getAttributes().get(pos).toString())>=0){
					if(m+1==tuples.size()||nKey.compareTo(tuples.get(m+1).getAttributes().get(pos).toString())<=0){
						insertIntoTVector(x,m+1);
					}else{
						lower=m;
					}
				}else{
					upper=m-1;
				}
			}
		}
	}
	public void serialize() throws IOException{
		FileOutputStream fileOut = new FileOutputStream(pageName+".ser");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(this);
		out.close();
		fileOut.close();
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
    public Tuple updateDesiredTuple(Object strKey,Hashtable<String, Object> htblColNameValue) {
    	return new Tuple(); //TODO
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
