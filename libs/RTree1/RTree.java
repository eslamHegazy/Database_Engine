package RTree1;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import General.GeneralReference;
import General.OverflowReference;
import General.Ref;
import kalabalaDB.DBAppException;
import kalabalaDB.Polygons;
import kalabalaDB.TreeIndex;

public class RTree<T extends Comparable<T>> implements Serializable,TreeIndex<T>{

	/**
	 * 
	 */
	//TODO meta data containing treename accompained with lastin
	//protected String treeName;
	protected static final long serialVersionUID = 1L;
	protected int order;
	protected RTreeNode<T> root;
	protected int lastin; 
	
	/**
	 * Creates an empty B+ tree
	 * @param order the maximum number of keys in the nodes of the tree
	 * @throws IOException 
	 * @throws DBAppException 
	 */
	public RTree(int order) throws DBAppException, IOException  
	{	
		this.order = order;
		//this.treeName=treeName;
		root = new RTreeLeafNode<T>(this.order);
		root.setRoot(true);
		//root.treeName=this.treeName;
	}
	public void updateRef(String oldpage,String newpage,T key,int tableNameLength) throws DBAppException, IOException {
		GeneralReference gf=search(key);
		gf.updateRef(oldpage, newpage, tableNameLength);
	}
	
	
	/**
	 * Inserts the specified key associated with the given record in the B+ tree
	 * @param key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 * @throws IOException 
	 * @throws DBAppException 
	 */
	public void insert(T key, Ref recordReference) throws DBAppException, IOException
	{
		PushUp<T> pushUp = root.insert(key, recordReference, null, -1);
		if(pushUp != null)
		{
			RTreeInnerNode<T> newRoot = new RTreeInnerNode<T>(order);
			root.serializeNode();
			newRoot.insertLeftAt(0, pushUp.key, root);
			root.deserializeNode(root.nodeName);
			newRoot.setChild(1, pushUp.newNode);
			root.setRoot(false);
			root.serializeNode();
			root = newRoot;
			root.setRoot(true);
		}
	}
	/**
	 * Looks up for the record that is associated with the specified key
	 * @param key the key to find its record
	 * @return the reference of the record associated with this key 
	 * @throws DBAppException 
	 */
	public GeneralReference search(T key) throws DBAppException
	{
		return root.search(key);
	}
	
	/**
	 * Delete a key and its associated record from the tree.
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
	 * @throws DBAppException 
	 */
	public boolean delete(T key) throws DBAppException
	{
		boolean done = root.delete(key, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof RTreeInnerNode && !root.isRoot())
			root = ((RTreeInnerNode<T>) root).getFirstChild();
		return done;
	}
	// to delete Ref only not the key
	public boolean delete(T key, String Page_name) throws DBAppException, IOException {
		boolean done = root.delete(key, null, -1,Page_name);
		//go down and find the new root in case the old root is deleted
		while(root instanceof RTreeInnerNode && !root.isRoot())
			root = ((RTreeInnerNode<T>) root).getFirstChild();
		return done;
	}
	
	/**
	 * Returns a string representation of the B+ tree.
	 */
	public String toString()
	{	
		
		//	<For Testing>
		// node :  (id)[k1|k2|k3|k4]{P1,P2,P3,}
		String s = "";
		RTreeLeafNode.pagesToPrint = new ArrayList<OverflowReference>();
		
		Queue<RTreeNode<T>> cur = new LinkedList<RTreeNode<T>>(), next;
		cur.add(root);
		while(!cur.isEmpty())
		{
			next = new LinkedList<RTreeNode<T>>();
			while(!cur.isEmpty())
			{
				RTreeNode<T> curNode = cur.remove();
				System.out.print(curNode);
				if(curNode instanceof RTreeLeafNode)
					System.out.print("->");
				else
				{
					System.out.print("{");
					RTreeInnerNode<T> parent = (RTreeInnerNode<T>) curNode;
					for(int i = 0; i <= parent.numberOfKeys; ++i)
					{			
						try 
						{
							System.out.print(parent.getChild(i).index+",");
							next.add(parent.getChild(i));
						}
						catch (DBAppException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					System.out.print("} ");
				}
				
			}
			System.out.println();
			cur = next;
		}
		
		ArrayList<OverflowReference> tobePrinted  = RTreeLeafNode.pagesToPrint;
		System.out.println("\n The Overflow refrences are : \n");
		
		for(int i=0;i<tobePrinted.size();i++)
		{
			System.out.println("refrence number : " + (i+1) +" is :\n");
			System.out.println(tobePrinted.get(i).toString());
		}
			
		//	</For Testing>
		return s;
	}
	public Ref searchForInsertion(T key) throws DBAppException { //comparable and T???
		return root.searchForInsertion(key);
	}
	public Ref searchRequiredReference(T key)throws DBAppException{
		search(key);
		return null;
		
	}
	public RTreeLeafNode getLeftmostLeaf() throws DBAppException {
		RTreeNode<T> curNode=root;
		while(!(curNode instanceof RTreeLeafNode)) {
			RTreeInnerNode <T> x=(RTreeInnerNode)curNode;
			curNode=x.getFirstChild();
		}
        
		return (RTreeLeafNode) curNode;
	}
	
}
