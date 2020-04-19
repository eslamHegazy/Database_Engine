package General;

import java.io.IOException;

import BPTree.BPTreeLeafNode;
import kalabalaDB.DBAppException;

public interface LeafNode<T extends Comparable<T>> {

	public LeafNode getNext() throws DBAppException;
	public int getNumberOfKeys();
	public Comparable<T> getKey(int index);
	public GeneralReference getRecord(int i);
	
	public LeafNode<T> searchForUpdateRef(T key);
	public void updateRef(String oldpage,String newpage,T key) throws DBAppException;
}
