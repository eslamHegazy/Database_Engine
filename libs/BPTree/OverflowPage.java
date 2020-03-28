package BPTree;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

import kalabalaDB.DBAppException;
import kalabalaDB.Page;

//TODO serializing overflowpages
public class OverflowPage implements Serializable{
	private String next;
	private Vector<Ref> refs;
	private int maxSize;// node size
	private String pageName;
	//private String treeName;
	public OverflowPage(int maxSize) throws DBAppException, IOException {
		this.maxSize=maxSize;
//		refs = new RecordReference[maxSize];
		refs = new Vector<Ref>(maxSize);
		next = null;
		//treeName=tree;
		String lastin=getFromMetaDataTree();
		pageName="Node"+lastin;	
	} 
	public Vector<Ref> getRefs() {
		return refs;
	}
	public void setRefs(Vector<Ref> refs) {
		this.refs = refs;
	}
	public void addRecord(Ref recordRef) throws DBAppException, IOException {
		if (refs.size()<maxSize) 
		{
			refs.add(recordRef);
		}
		else {
			OverflowPage nextPage;
			if (next==null) 
			{
				nextPage = new OverflowPage(maxSize);
				next=nextPage.getPageName();	
			}else{
				nextPage=deserialize(next);
			}
			
			nextPage.addRecord(recordRef);
			nextPage.serialize();
		}
	}
	
	public OverflowPage getNext() throws DBAppException {
		if(next.equals(""))
			return null;
		return deserialize(next);
	}
	public void setNext(String next) {
		this.next = next;
	}
	public String getPageName() {
		return pageName;
	}
	public boolean updateRef(String oldpage, String newpage) throws DBAppException  {
		int i=0;
		for (;i<refs.size()&&refs.get(i).getPage()<=oldpage;i++);
		//i--;
		if (i==0) {
			return false;
		}
		if (i<refs.size()) {
			refs.get(i-1).setPage(newpage);
		}
		if (i==refs.size()) {
			OverflowPage nextPage=deserialize(next);
			if (nextPage!=null && nextPage.updateRef(oldpage, newpage)) {
				nextPage.serialize();
				return true;
			}
			else {
				refs.get(i-1).setPage(newpage);
			}
		}
		return false;
	}
	
	public void serialize() throws DBAppException{
		try {
			FileOutputStream fileOut = new FileOutputStream("data/"+ this.getPageName() + ".class"); //TODO  l name 
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(this);
			out.close();
			fileOut.close();
		}
		catch(IOException e) {
			throw new DBAppException("IO Exception");
		}
	}
	public OverflowPage deserialize(String name) throws DBAppException{ 
		try {
			FileInputStream fileIn = new FileInputStream("data/"+ name + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			OverflowPage OFP = (OverflowPage) in.readObject();
			in.close();
			fileIn.close();
			return OFP;
		}
		catch(IOException e) {
			throw new DBAppException("IO Exception");
		}
		catch(ClassNotFoundException e) {
			throw new DBAppException("Class Not Found Exception");
		}
		 
	}
	public static Vector readFile(String path) throws DBAppException {
		try {
			String currentLine = "";
			FileReader fileReader = new FileReader(path);
			BufferedReader br = new BufferedReader(fileReader);
			Vector metadata = new Vector();
			while ((currentLine = br.readLine()) != null) {
				metadata.add(currentLine.split(","));
			}
			return metadata;
		}
		catch(IOException e) {
			e.printStackTrace();
			throw new DBAppException("IO Exception");
		}
	}

	protected String getFromMetaDataTree() throws DBAppException , IOException
	{
		String lastin = "";
		Vector meta = readFile("data/metaBPtree.csv");
		int overrideLastin = 0;
		for (Object O : meta) {
			String[] curr = (String[]) O;
			lastin = curr[0];
			overrideLastin = Integer.parseInt(curr[0])+1;
			curr[0] = overrideLastin + "";
			break;
			
		}
		FileWriter csvWriter = new FileWriter("data/metaBPtree.csv");
		for (Object O : meta) {
			String[] curr = (String[]) O;
			csvWriter.append(curr[0]);
			break;
		}
		csvWriter.flush();
		csvWriter.close();
		return lastin;
	}
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("The overflow page:" + pageName + ": \n");
		for(Ref r : refs)
		{
			sb.append(r+" , ");
		}
		sb.append("\n");
		if(this.next == null)
			return sb.toString();
		try 
		{
			sb.append(deserialize(next).toString());
		}
		catch(DBAppException e)
		{
			System.out.println("PAGE EXCEPTION");
		}
		return sb.toString();
		
	}
	
}
