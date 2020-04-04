package BPTree;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import kalabalaDB.DBAppException;

public class BPTreeLeafNode<T extends Comparable<T>> extends BPTreeNode<T> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private GeneralReference[] records;
	private String next;
	@SuppressWarnings("unchecked")
	public BPTreeLeafNode(int n) throws DBAppException, IOException 
	{
		super(n);
		keys = new Comparable[n];
		records = new GeneralReference[n];

	}
	
	/**
	 * @return the next leaf node
	 * @throws DBAppException 
	 */
	public BPTreeLeafNode<T> getNext() throws DBAppException
	{
		return (next==null)?null:((BPTreeLeafNode)deserializeNode(next)); //TODO wth
	}
	
	/**
	 * sets the next leaf node
	 * @param node the next leaf node
	 */
	public void setNext(BPTreeLeafNode<T> node)
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
	 * insert the specified key associated with a given record refernce in the B+ tree
	 * @throws IOException 
	 * @throws DBAppException 
	 */
	public PushUp<T> insert(T key, 
			Ref recordReference, 
			BPTreeInnerNode<T> parent, 
			int ptr) throws DBAppException, IOException
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
			BPTreeNode<T> newNode = this.split(key, recordReference);
			Comparable<T> newKey = newNode.getFirstKey();
			newNode.serializeNode(); //TODO type cast or create in BPTreeNode
			return new PushUp<T>(newNode, newKey);
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
	private void insertAt(int index, Comparable<T> key, GeneralReference generalReference) 
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
	 * @throws IOException 
	 * @throws DBAppException 
	 */
	public BPTreeNode<T> split(T key, GeneralReference recordReference) throws DBAppException, IOException 
	{
		int keyIndex = this.findIndex(key);
		int midIndex = numberOfKeys / 2;
		if((numberOfKeys & 1) == 1 && keyIndex > midIndex)	//split nodes evenly
			++midIndex;		

		
		int totalKeys = numberOfKeys + 1;
		//move keys to a new node
		BPTreeLeafNode<T> newNode = new BPTreeLeafNode<T>(order);
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
	public int findIndex(T key) 
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
	public GeneralReference search(T key) 
	{
		for(int i = 0; i < numberOfKeys; ++i)
			if(this.getKey(i).compareTo(key) == 0)
				return this.getRecord(i);
		return null;
	}
	public Ref searchForInsertion(T key)throws DBAppException
	{
		for(int i = 0; i < numberOfKeys; ++i)
			if(this.getKey(i).compareTo(key) > 0)
				return (Ref)(this.getRecord(i));
		return (Ref)(records[records.length-1]);
	}
	/**
	 * delete the passed key from the B+ tree
	 * @throws DBAppException 
	 */
	public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr) throws DBAppException 
	{
		for(int i = 0; i < numberOfKeys; ++i)
			if(keys[i].compareTo(key) == 0)
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
	public boolean borrow(BPTreeInnerNode<T> parent, int ptr) throws DBAppException
	{
		//check left sibling
		if(ptr > 0)
		{
			BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
			if(leftSibling.numberOfKeys > leftSibling.minKeys())
			{
				this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());		
				leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
				parent.setKey(ptr - 1, keys[0]);
				return true;
			}
		}
		
		//check right sibling
		if(ptr < parent.numberOfKeys)
		{
			BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
			if(rightSibling.numberOfKeys > rightSibling.minKeys())
			{
				this.insertAt(numberOfKeys, rightSibling.getFirstKey(), rightSibling.getFirstRecord());
				rightSibling.deleteAt(0);
				parent.setKey(ptr, rightSibling.getFirstKey());
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
	public void merge(BPTreeInnerNode<T> parent, int ptr) throws DBAppException
	{
		if(ptr > 0)
		{
			//merge with left
			BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
			leftSibling.merge(this);
			parent.deleteAt(ptr-1);			
		}
		else
		{
			//merge with right
			BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
			this.merge(rightSibling);
			parent.deleteAt(ptr);
		}
	}
	
	/**
	 * merge the current node with the specified node. The foreign node will be deleted
	 * @param foreignNode the node to be merged with the current node
	 * @throws DBAppException 
	 */
	public void merge(BPTreeLeafNode<T> foreignNode) throws DBAppException
	{
		for(int i = 0; i < foreignNode.numberOfKeys; ++i)
			this.insertAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getRecord(i));
		
		this.setNext(foreignNode.getNext());
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

	
	
	


	
}
