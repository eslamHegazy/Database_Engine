package RTree;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;


import General.GeneralReference;
import General.Ref;
import kalabalaDB.DBAppException;

public abstract class RTreeNode<Polygons extends Comparable<Polygons>> implements Serializable{
	
	/**
	 * Abstract class that collects the common functionalities of the inner and leaf nodes
	 */
	private static final long serialVersionUID = 1L;
	protected Comparable<Polygons>[] keys;
	protected int numberOfKeys;
	protected int order;
	protected int index;		//for printing the tree
	private boolean isRoot;
	private static int nextIdx = 0;
	public int getNumberOfKeys() {
		return numberOfKeys;
	}

	public void setNumberOfKeys(int numberOfKeys) {
		this.numberOfKeys = numberOfKeys;
	}
	protected String nodeName;
	//protected int lastin;
	//protected String treeName;

	public RTreeNode(int order) throws DBAppException 
	{
		index = nextIdx++;
		numberOfKeys = 0;
		this.order = order;
		nodeName=getFromMetaDataTree();
		//nodeName=treeName+lastin;
	}
	
	public static Vector readFile(String path) throws DBAppException 
	{
		try 
		{
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
			throw new DBAppException("IO Exception while reading file: "+path);
		}
	}

	protected String getFromMetaDataTree() throws DBAppException 
	{
		try {

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
			for (Object O : meta) 
			{
				String[] curr = (String[]) O;
				csvWriter.append(curr[0]);
				break;
			}
			csvWriter.flush();
			csvWriter.close();
			return lastin;
		}
		catch (IOException e) {
			e.printStackTrace();
			throw new DBAppException("IOException while reading metaBPtree.csv in order to write a node/overflowpage to disk");
		}
	}
	
	
	
	
	/**
	 * @return a boolean indicating whether this node is the root of the R tree
	 */
	public boolean isRoot()
	{
		return isRoot;
	}
	
	/**
	 * set this node to be a root or unset it if it is a root
	 * @param isRoot the setting of the node
	 */
	public void setRoot(boolean isRoot)
	{
		this.isRoot = isRoot;
	}
	
	/**
	 * find the key at the specified index
	 * @param index the index at which the key is located
	 * @return the key which is located at the specified index
	 */
	public Comparable<Polygons> getKey(int index) 
	{
		return keys[index];
	}

	/**
	 * sets the value of the key at the specified index
	 * @param index the index of the key to be set
	 * @param key the new value for the key
	 */
	public void setKey(int index, Comparable<Polygons> key) 
	{
		keys[index] = key;
	}
	
	/**
	 * @return a boolean whether this node is full or not
	 */
	public boolean isFull() 
	{
		return numberOfKeys == order;
	}
	
	/**
	 * @return the last key in this node
	 */
	public Comparable<Polygons> getLastKey()
	{
		return keys[numberOfKeys-1];
	}
	
	/**
	 * @return the first key in this node
	 */
	public Comparable<Polygons> getFirstKey()
	{
		return keys[0];
	}
	
	/**
	 * @return the minimum number of keys this node can hold
	 */
	public abstract int minKeys();

	/**
	 * insert a key with the associated record reference in the R tree
	 * @param key the key to be inserted
	 * @param recordReference a pointer to the record on the hard disk
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node
	 * @return a key and a new node in case of a node splitting and null otherwise 
	 * @throws DBAppException 
	 */
	public abstract PushUp<Polygons> insert(Polygons key, Ref recordReference, RTreeInnerNode<Polygons> parent, int ptr) throws DBAppException;
	public abstract GeneralReference search(Polygons key) throws DBAppException;
	public abstract ArrayList<GeneralReference> searchMT(Polygons key)throws DBAppException;
	public abstract ArrayList<GeneralReference> searchMTE(Polygons key)throws DBAppException;
	public abstract Ref searchForInsertion(Polygons key,int tableLength)throws DBAppException;

	/**
	 * delete a key from the R tree recursively
	 * @param key the key to be deleted from the R tree
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 * @return true if this node was successfully deleted and false otherwise
	 * @throws DBAppException 
	 */
	public abstract boolean delete(Polygons key, RTreeInnerNode<Polygons> parent, int ptr) throws DBAppException;
	public abstract boolean delete(Polygons key, RTreeInnerNode<Polygons> parent, int ptr,String page_name) throws DBAppException;
	
	/**
	 * A string represetation for the node
	 */
	public String toString()
	{		
		String s = "(" + index + ")";

		s += "[";
		for (int i = 0; i < order; i++)
		{
			String key = " ";
			if(i < numberOfKeys)
				key = keys[i].toString();
			
			s+= key;
			if(i < order - 1)
				s += "|";
		}
		s += "]";
		return s;
	}
		

	
	public void serializeNode() throws DBAppException 
	{
		try 
		{
			System.out.println("IO||||\t serialize:node:"+nodeName);
			FileOutputStream fileOut = new FileOutputStream("data/"+ this.nodeName+ ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(this);
			out.close();
			fileOut.close();
		}
		catch(IOException e) {
			e.printStackTrace();
			throw new DBAppException("IO Exception while writing a node to the disk\"+\"\\tdata/\"+name+\".class\");");
		}
		
	}
	public RTreeNode<Polygons> deserializeNode(String name) throws DBAppException {
		try {
			System.out.println("IO||||\t deserialize:node:"+name);
		//	if(name == null || name == "")
		//		return null;
			FileInputStream fileIn = new FileInputStream("data/"+ name + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			RTreeNode<Polygons> RTN =   (RTreeNode<Polygons>) in.readObject();
			in.close();
			fileIn.close();
			return RTN;
		}
		catch(IOException e) {
			throw new DBAppException("IO Exception while loading a node from the disk"+"\tdata/"+name+".class");
		}
		catch(ClassNotFoundException e) {
			throw new DBAppException("Class Not Found Exception");
		}
		
	}

	
	public abstract RTreeLeafNode searchForUpdateRef(Polygons key) throws DBAppException;
	

}
