package RTree;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import BPTree.BPTreeInnerNode;
import BPTree.BPTreeLeafNode;
import BPTree.BPTreeNode;
import General.GeneralReference;
import General.OverflowReference;
import General.Ref;
import General.TreeIndex;
import kalabalaDB.DBAppException;
import kalabalaDB.Polygons;

public class RTree<Polygons extends Comparable<Polygons>> implements Serializable,TreeIndex<Polygons>{

	/**
	 * 
	 */
	//TODO meta data containing treename accompained with lastin
	//protected String treeName;
	protected static final long serialVersionUID = 1L;
	protected int order;
	protected RTreeNode<Polygons> root;
	protected int lastin; 
	
	/**
	 * Creates an empty R tree
	 * @param order the maximum number of keys in the nodes of the tree
	 * @throws IOException 
	 * @throws DBAppException 
	 */
	public RTree(int order) throws DBAppException, IOException  
	{	
		this.order = order;
		//this.treeName=treeName;
		root = new RTreeLeafNode<Polygons>(this.order);
		root.setRoot(true);
		//root.treeName=this.treeName;
	}
	public void updateRef(String oldpage,String newpage,Polygons key,int tableNameLength) throws DBAppException, IOException {
//		GeneralReference gf=search(key);
//		gf.updateRef(oldpage, newpage, tableNameLength);

		RTreeLeafNode leaf = searchForUpdateRef(key);
		leaf.updateRef(oldpage,newpage,key,tableNameLength);
		
		leaf.serializeNode();
	}
	public RTreeLeafNode searchForUpdateRef(Polygons key) throws DBAppException{
		return root.searchForUpdateRef(key);
	}
	
	
	
	/**
	 * Inserts the specified key associated with the given record in the R tree
	 * @param key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 * @throws IOException 
	 * @throws DBAppException 
	 */
	public void insert(Polygons key, Ref recordReference) throws DBAppException, IOException
	{
		PushUp<Polygons> pushUp = root.insert(key, recordReference, null, -1);
		if(pushUp != null)
		{
			RTreeInnerNode<Polygons> newRoot = new RTreeInnerNode<Polygons>(order);
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
	public GeneralReference search(Polygons key) throws DBAppException
	{
		return root.search(key);
	}
	
	/**
	 * Delete a key and its associated record from the tree.
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
	 * @throws DBAppException 
	 */
	public boolean delete(Polygons key) throws DBAppException
	{
		boolean done = root.delete(key, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof RTreeInnerNode && !root.isRoot())
			root = ((RTreeInnerNode<Polygons>) root).getFirstChild();
		return done;
	}
	// to delete Ref only not the key
	public boolean delete(Polygons key, String Page_name) throws DBAppException, IOException {
		boolean done = root.delete(key, null, -1,Page_name);
		//go down and find the new root in case the old root is deleted
		while(root instanceof RTreeInnerNode && !root.isRoot())
			root = ((RTreeInnerNode<Polygons>) root).getFirstChild();
		return done;
	}
	
	/**
	 * Returns a string representation of the R tree.
	 */
	public String toString()
	{	
		
		//	<For Testing>
		// node :  (id)[k1|k2|k3|k4]{P1,P2,P3,}
		StringBuilder sb = new StringBuilder();
		BPTreeLeafNode.pagesToPrint = new ArrayList<OverflowReference>();
		
		Queue<RTreeNode> cur = new LinkedList<RTreeNode>(), next;
		cur.add(root);
		while(!cur.isEmpty())
		{
			next = new LinkedList<RTreeNode>();
			while(!cur.isEmpty())
			{
				RTreeNode curNode = cur.remove();
//				System.out.print(curNode);
				sb.append(curNode);
				if(curNode instanceof RTreeLeafNode)
//					System.out.print("->");
					sb.append("->");
				else
				{
//					System.out.print("{");
					sb.append("{");
					RTreeInnerNode parent = (RTreeInnerNode) curNode;
					for(int i = 0; i <= parent.numberOfKeys; ++i)
					{			
						try 
						{
//							System.out.print(parent.getChild(i).index+",");
							sb.append(parent.getChild(i).index+",");
							next.add(parent.getChild(i));
						}
						catch (DBAppException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
//					System.out.print("} ");
					sb.append("}");
				}
				
			}
//			System.out.println();
			sb.append("\n");
			cur = next;
		}
		
		ArrayList<OverflowReference> tobePrinted  = BPTreeLeafNode.pagesToPrint;
//		System.out.println("\n The Overflow refrences are : \n");
		sb.append("\n The Overflow refrences are : \n");
		
		for(int i=0;i<tobePrinted.size();i++)
		{
//			System.out.println("refrence number : " + (i+1) +" is :\n");
//			System.out.println(tobePrinted.get(i).toString());
			sb.append("refrence number : " + (i+1) +" is :\n");
			sb.append(tobePrinted.get(i).toString());
		}
			
		//	</For Testing>
		return sb.toString();
	}
	
	public Ref searchForInsertion(Polygons key,int tableLength) throws DBAppException { //comparable and T???
		return root.searchForInsertion(key, tableLength);
	}
	public Ref searchRequiredReference(Polygons key)throws DBAppException{
		search(key);
		return null;
		
	}
	public RTreeLeafNode getLeftmostLeaf() throws DBAppException {
		RTreeNode<Polygons> curNode=root;
		while(!(curNode instanceof RTreeLeafNode)) {
			RTreeInnerNode <Polygons> x=(RTreeInnerNode)curNode;
			curNode=x.getFirstChild();
		}
        
		return (RTreeLeafNode) curNode;
	}
	@Override
	public ArrayList<GeneralReference> searchMTE(Polygons key) throws DBAppException {
		return root.searchMTE(key);
	}
	@Override
	public ArrayList<GeneralReference> searchMT(Polygons key) throws DBAppException {
		return root.searchMT(key);
	}
//	public ArrayList<GeneralReference> searcHlTE(Polygons key) throws DBAppException {
//		return root.searchLTE(key);
//	}
//	@Override
//	public ArrayList<GeneralReference> searchlT(Polygons key) throws DBAppException {
//		return root.searchLT(key);
//	}
	
}
