package RTree;

import java.io.Serializable;
import java.util.ArrayList;

import General.GeneralReference;
import General.LeafNode;
import General.OverflowPage;
import General.OverflowReference;
import General.Ref;
import kalabalaDB.DBAppException;

public class RTreeLeafNode<Polygons extends Comparable<Polygons>> extends RTreeNode<Polygons> implements Serializable,LeafNode<Polygons>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private GeneralReference[] records;
	private String next;
	@SuppressWarnings("unchecked")
	public RTreeLeafNode(int n) throws DBAppException 
	{
		super(n);
		keys = new Comparable[n];
		records = new GeneralReference[n];

	}
	
	/**
	 * @return the next leaf node
	 * @throws DBAppException 
	 */
	public RTreeLeafNode<Polygons> getNext() throws DBAppException
	{
		return (next==null)?null:((RTreeLeafNode)deserializeNode(next)); //TODO wth
	}
	
	/**
	 * sets the next leaf node
	 * @param node the next leaf node
	 */
	public void setNext(RTreeLeafNode<Polygons> node)
	{
		this.next = (node!=null)?node.nodeName:null;
	}
	
	/**
	 * @param index the index to find its record
	 * @return the reference of the queried index
	 */
	public GeneralReference getRecord(int index) 
	{
		return records[index];
	}
	
	/**
	 * sets the record at the given index with the passed reference
	 * @param index the index to set the value at
	 * @param recordReference the reference to the record
	 */
	public void setRecord(int index, GeneralReference recordReference) 
	{
		records[index] = recordReference;
	}
    
	/**
	 * @return the reference of the last record
	 */
	public GeneralReference getFirstRecord()
	{
		return records[0];
	}

	/**
	 * @return the reference of the last record
	 */
	public GeneralReference getLastRecord()
	{
		return records[numberOfKeys-1];
	}
	
	/**
	 * finds the minimum number of keys the current node must hold
	 */
	public int minKeys()
	{
		if(this.isRoot())
			return 1;
		return (order + 1) / 2;
	}
	
	/**
	 * insert the specified key associated with a given record refernce in the R tree
	 * @throws DBAppException 
	 */
	public PushUp<Polygons> insert(Polygons key, 
			Ref recordReference, 
			RTreeInnerNode<Polygons> parent, 
			int ptr) throws DBAppException
	{
			
		int index = 0;
		while (index < numberOfKeys && getKey(index).compareTo(key) < 0)
			++index;
		
		if (index< numberOfKeys && getKey(index).compareTo(key)==0) {
			GeneralReference ref = records[index];
			if (ref.isOverflow()) {
				//done:
				
				//deserialize the actual page of this reference
				OverflowReference ofRef=(OverflowReference)ref;
				ofRef.insert(recordReference);
				//insert key in the overflow page
				//ovflpage.addRecord(recordReference);
				
				//serialize overflow page
			}
			else {
				OverflowReference ovflref = new OverflowReference();
				OverflowPage ovflpage = new OverflowPage(order);
				ovflref.setFirstPage(ovflpage);
				//done: what to store in ovflref? (string/ovflPage!?)
				ovflref.insert((Ref)ref);
				ovflref.insert(recordReference);
				records[index]=ovflref;
			}
			//this.serialize();
			return null;
		}
		
		else if(this.isFull())
		{
			RTreeNode<Polygons> newNode = this.split(key, recordReference);
			Comparable<Polygons> newKey = newNode.getFirstKey();
			newNode.serializeNode(); //TODO type cast or create in RTreeNode
			return new PushUp<Polygons>(newNode, newKey);
		}
		else
		{
			this.insertAt(index, key, recordReference);
			return null;
		}
	}
	
	/**
	 * inserts the passed key associated with its record reference in the specified index
	 * @param index the index at which the key will be inserted
	 * @param key the key to be inserted
	 * @param generalReference the pointer to the record associated with the key
	 */
	private void insertAt(int index, Comparable<Polygons> key, GeneralReference generalReference) 
	{
		for (int i = numberOfKeys - 1; i >= index; --i) 
		{
			this.setKey(i + 1, getKey(i));
			this.setRecord(i + 1, getRecord(i));
		}

		this.setKey(index, key);
		this.setRecord(index, generalReference);
		++numberOfKeys;
	}
	
	/**
	 * splits the current node
	 * @param key the new key that caused the split
	 * @param recordReference the reference of the new key
	 * @return the new node that results from the split
	 * @throws DBAppException 
	 */
	public RTreeNode<Polygons> split(Polygons key, GeneralReference recordReference) throws DBAppException 
	{
		int keyIndex = this.findIndex(key);
		int midIndex = numberOfKeys / 2;
		if((numberOfKeys & 1) == 1 && keyIndex > midIndex)	//split nodes evenly
			++midIndex;		

		
		int totalKeys = numberOfKeys + 1;
		//move keys to a new node
		RTreeLeafNode<Polygons> newNode = new RTreeLeafNode<Polygons>(order);
		for (int i = midIndex; i < totalKeys - 1; ++i) 
		{
			newNode.insertAt(i - midIndex, this.getKey(i), this.getRecord(i));
			numberOfKeys--;
		}
		
		//insert the new key
		if(keyIndex < totalKeys / 2)
			this.insertAt(keyIndex, key, recordReference);
		else
			newNode.insertAt(keyIndex - midIndex, key, recordReference);
		
		//set next pointers
		newNode.setNext(this.getNext());
		if(this.getNext() != null)
			this.getNext().serializeNode();
		this.setNext(newNode);
		
		return newNode;
	}
	
	/**
	 * finds the index at which the passed key must be located 
	 * @param key the key to be checked for its location
	 * @return the expected index of the key
	 */
	public int findIndex(Polygons key) 
	{
		for (int i = 0; i < numberOfKeys; ++i) 
		{
			int cmp = getKey(i).compareTo(key);
			if (cmp > 0) 
				return i;
		}
		return numberOfKeys;
	}

	/**
	 * returns the record reference with the passed key and null if does not exist
	 */
	@Override
	public GeneralReference search(Polygons key) 
	{
		for(int i = 0; i < numberOfKeys; ++i)
			if(this.getKey(i).compareTo(key)==0)
				return this.getRecord(i);
		return null;
	}
	public Ref searchForInsertion(Polygons key,int tableLength)throws DBAppException
	{
		int i=0;
		for(; i < numberOfKeys; i++){
			if(this.getKey(i).compareTo(key) >= 0)
				return this.refReference((this.getRecord(i)),tableLength);
		}	
		if(i>0){
			return this.refReference(this.getRecord(i-1),tableLength);
		}
		return null;
	}
	private Ref refReference(GeneralReference generalReference,int tableLength) throws DBAppException {
		if(generalReference instanceof Ref){
			return (Ref)generalReference;
		}else{
				OverflowReference o=(OverflowReference)generalReference;
				String pageName=o.getFirstPageName();
					OverflowPage p=o.deserializeOverflowPage(pageName);
					Ref r=p.getMaxRefPage(tableLength);
					p.serialize();	//TODO
					return r;
//					while(pageName!=null){
//						p.serialize();
//						p=o.deserializeOverflowPage(pageName);
//						pageName=p.getNext();
//					}
//					Ref r=p.getRefs().get(p.getRefs().size()-1);
//					p.serialize();
//					return r;
					
				
		}
	}
	/**
	 * delete the passed key from the R tree
	 * @throws DBAppException 
	 */
	public boolean delete(Polygons key, RTreeInnerNode<Polygons> parent, int ptr) throws DBAppException 
	{
		for(int i = 0; i < numberOfKeys; ++i)
//			if(keys[i].compareTo(key) == 0)
			if(keys[i].compareTo(key)==0)
			{
				this.deleteAt(i);
				if(i == 0 && ptr > 0)
				{
					//update key at parent
					parent.setKey(ptr - 1, this.getFirstKey());
				}
				//check that node has enough keys
				if(!this.isRoot() && numberOfKeys < this.minKeys())
				{
					//1.try to borrow
					if(borrow(parent, ptr))
						return true;
					//2.merge
					merge(parent, ptr);
				}
				return true;
			}
		return false;
	}
	
	
	// to delete a ref not a page
	
	public boolean delete(Polygons key, RTreeInnerNode<Polygons> parent, int ptr,String page_name) throws DBAppException 
	{
		for(int i = 0; i < numberOfKeys; ++i)
//			if(keys[i].compareTo(key) == 0)
			if(keys[i].compareTo(key)==0)
			{
				// handle deleting only one ref not the entire key
				if(records[i] instanceof Ref)
					this.deleteAt(i);//didn't serialize yet
				else
				{
					OverflowReference ov = (OverflowReference) records[i];
					ov.deleteRef(page_name);
					if(ov.getTotalSize() == 1)
					{
						OverflowPage firstpage = ov.deserializeOverflowPage(ov.getFirstPageName());
						Ref r = firstpage.getRefs().firstElement();
						records[i] = r;
						// TODO delete the overflow page from DISK  
					}
					
				}
				
				
				if(i == 0 && ptr > 0)
				{
					//update key at parent
					parent.setKey(ptr - 1, this.getFirstKey());
					//TODO:parent isn't serialized yet
				}
				//check that node has enough keys
				if(!this.isRoot() && numberOfKeys < this.minKeys())
				{
					//1.try to borrow
					if(borrow(parent, ptr)) {
						//this node isn't serialized yet; just left/right sibiling
						parent.serializeNode();
						return true;
					}
					//2.merge
					merge(parent, ptr);
					parent.serializeNode();
				}
				return true;
			}
		return false;
	}
	
	/**
	 * delete a key at the specified index of the node
	 * @param index the index of the key to be deleted
	 */
	public void deleteAt(int index)
	{
		//check if there is an overflow
		for(int i = index; i < numberOfKeys - 1; ++i)
		{
			keys[i] = keys[i+1];
			records[i] = records[i+1];
		}
		numberOfKeys--;
	}
	
	/**
	 * tries to borrow a key from the left or right sibling
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 * @return true if borrow is done successfully and false otherwise
	 * @throws DBAppException 
	 */
	public boolean borrow(RTreeInnerNode<Polygons> parent, int ptr) throws DBAppException
	{
		//check left sibling
		if(ptr > 0)
		{
			RTreeLeafNode<Polygons> leftSibling = (RTreeLeafNode<Polygons>) parent.getChild(ptr-1);
			if(leftSibling.numberOfKeys > leftSibling.minKeys())
			{
				this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());		
				leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
				parent.setKey(ptr - 1, keys[0]);
				leftSibling.serializeNode();
				return true;
			}
		}
		
		//check right sibling
		if(ptr < parent.numberOfKeys)
		{
			RTreeLeafNode<Polygons> rightSibling = (RTreeLeafNode<Polygons>) parent.getChild(ptr+1);
			if(rightSibling.numberOfKeys > rightSibling.minKeys())
			{
				this.insertAt(numberOfKeys, rightSibling.getFirstKey(), rightSibling.getFirstRecord());
				rightSibling.deleteAt(0);
				parent.setKey(ptr, rightSibling.getFirstKey());
				rightSibling.serializeNode();
				return true;
			}
		}
		return false;
	}
	
	/**
	 * merges the current node with its left or right sibling
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 * @throws DBAppException 
	 */
	public void merge(RTreeInnerNode<Polygons> parent, int ptr) throws DBAppException
	{
		if(ptr > 0)
		{
			//merge with left
			RTreeLeafNode<Polygons> leftSibling = (RTreeLeafNode<Polygons>) parent.getChild(ptr-1);
			leftSibling.merge(this);
			parent.deleteAt(ptr-1);			
		}
		else
		{
			//merge with right
			RTreeLeafNode<Polygons> rightSibling = (RTreeLeafNode<Polygons>) parent.getChild(ptr+1);
			this.merge(rightSibling);
			parent.deleteAt(ptr);
		}
	}
	
	/**
	 * merge the current node with the specified node. The foreign node will be deleted
	 * @param foreignNode the node to be merged with the current node
	 * @throws DBAppException 
	 */
	public void merge(RTreeLeafNode<Polygons> foreignNode) throws DBAppException
	{
		for(int i = 0; i < foreignNode.numberOfKeys; ++i)
			this.insertAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getRecord(i));
		
		this.setNext(foreignNode.getNext());
		//TODO: Have the removed node been deleted from disk ?
	}
	
	public static ArrayList<OverflowReference> pagesToPrint;
	public String toString()
	{		
		String s = "(" + index + ")";

		s += "[";
		for (int i = 0; i < order; i++)
		{
			String key = " ";
			if(i < numberOfKeys) {
				key = keys[i].toString();
				
				if(records[i] instanceof Ref)
				{
					key += ","+records[i]; 
				}
				
				else
				{
					key += ","+((OverflowReference)records[i]).getFirstPageName();
					if (pagesToPrint==null) pagesToPrint=new ArrayList<OverflowReference>();
					pagesToPrint.add((OverflowReference) records[i]);
				}
			
			}
			s+= key;
			if(i < order - 1)
				s += "|";
		}
		s += "]";
		return s;
	}
	
	
	public ArrayList<GeneralReference> searchMTE(Polygons key) throws DBAppException{
		ArrayList<GeneralReference> res = new ArrayList<GeneralReference>();
		searchMTE(key,res);
		return res;
	}
	public ArrayList<GeneralReference> searchMT(Polygons key)throws DBAppException{
		ArrayList<GeneralReference> res = new ArrayList<GeneralReference>();
		searchMT(key,res);
		return res;
	}
	
	public void searchMTE(Polygons key,ArrayList<GeneralReference> res)throws DBAppException{
		int i = 0;
		for(; i < numberOfKeys; ++i) {
			if(this.getKey(i).compareTo(key) >= 0)
				res.add(this.getRecord(i));
		}
		if ( next!=null){//don't need to check i==numberOfKeys because I am traversing till the end;rightmost leaf
			RTreeLeafNode nxt = (RTreeLeafNode)deserializeNode(next);
			nxt.searchMTE(key,res);
		}
		
	}
	public void searchMT(Polygons key, ArrayList<GeneralReference> res) throws DBAppException{
		for(int i=0; i < numberOfKeys; ++i)
			if(this.getKey(i).compareTo(key) > 0)
				res.add(this.getRecord(i));
//			else if (this.getKey(i).compareTo(key)==0)
//				break;
		if (next!=null) {
			RTreeLeafNode<Polygons> nxt = (RTreeLeafNode<Polygons>)deserializeNode(next);
			nxt.searchMT(key,res);
		}
	}

//	public void searchLTE(Polygons key,ArrayList<GeneralReference> res)throws DBAppException{
//		int i = 0;
//		boolean flag = true;
//		for(; i < numberOfKeys && flag; ++i) {
//			if(this.getKey(i).compareTo(key) <= 0) {
//				res.add(this.getRecord(i));
//			}
//			else {
//				flag = false;
//			}
//		}
//		if ( flag && next!=null){//don't need to check i==numberOfKeys because I am traversing till the end;rightmost leaf
//			RTreeLeafNode nxt = (RTreeLeafNode)deserializeNode(next);
//			nxt.searchMTE(key,res);
//		}
//		
//	}
//	public void searchLT(Polygons key, ArrayList<GeneralReference> res) throws DBAppException{
//		boolean flag = true;
//		for(int i=0; i < numberOfKeys && flag; ++i)
//			if(this.getKey(i).compareTo(key) < 0) {
//				res.add(this.getRecord(i));
//			}
//			else {
//				flag = false;
//			}
//		if (flag && next!=null) {
//			RTreeLeafNode nxt = (RTreeLeafNode)deserializeNode(next);
//			nxt.searchMT(key,res);
//		}
//	}

	public RTreeLeafNode searchForUpdateRef(Polygons key) {
		return this;
	}
	public void updateRef(String oldpage,String newpage,Polygons key,int tableNameLength) throws DBAppException{
		GeneralReference gf;
		for(int i = 0; i < numberOfKeys; ++i)
			if(this.getKey(i).compareTo(key)==0) {
				gf = getRecord(i);
				gf.updateRef(oldpage, newpage, tableNameLength);
				if (gf instanceof Ref) {
					this.serializeNode();
				}
				return;
			}
	}
	
	
}
