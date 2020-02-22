import java.io.*;
import java.util.*;

public class Table implements Serializable {
	private  Vector<String> pages= new Vector();
	private Vector<String> 	MinMax  = new Vector<>();
	private transient String tableName;
	private transient String strClusteringKey;
	
	public Vector<String> getPages() {
		return pages;
	}
	public void setPages(Vector<String> pages) {
		this.pages = pages;
	}
	public Vector<String> getMinMax() {
		return MinMax;
	}
	public void setMinMax(Vector<String> minMax) {
		MinMax = minMax;
	}
	public String getStrClusteringKey() {
		return strClusteringKey;
	}
	public void setStrClusteringKey(String strClusteringKey) {
		this.strClusteringKey = strClusteringKey;
	}
	private transient int primaryPos;
	
	//	private transient Vector colNames;
	//private transient Vector colTypes;

	public Table() {
		
	}
//	public void addToColNames(String s) {
//		colNames.add(s);
//	}
//	public Vector getColNames() {
//		return colNames;
//	}
//	public void addToColTypes(String s) {
//		colTypes.add(s);
//	}
//	public Vector getColTypes() {
//		return colTypes;
//	}
	public void setTableName(String name) {
		tableName=name;
	}
	public void setPrimaryPos(int pos) {
		primaryPos=pos;
	}
	public int getPrimaryPos() {
		return primaryPos;
	}
	public String getTableName() {
		return tableName;
	}
    public void insertSorted(Tuple x) {
    	//TODO
    }
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Page c = new Page();
		Tuple t=new Tuple();
		t.addAttribute("abdallah");
		t.addAttribute(2000);
		t.addAttribute(false);
        c.addTuple(t);
		Table xxx = new Table();
		xxx.pages.add(c.getPageName());

		FileOutputStream fileOut = new FileOutputStream("childSer.ser");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(xxx);
		out.close();
		fileOut.close();
		System.out.printf("Serialized data is done");

		FileInputStream fileIn = new FileInputStream("childSer.ser");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Object xx = in.readObject();
		in.close();
		fileIn.close();
		System.out.println(xxx);

	}
}
