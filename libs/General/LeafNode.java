package General;

import BPTree.BPTreeLeafNode;
import kalabalaDB.DBAppException;

public interface LeafNode<T extends Comparable<T>> {

	public LeafNode getNext() throws DBAppException;
	public int getNumberOfKeys();
	public Comparable<T> getKey(int index);
	public GeneralReference getRecord(int i);
	

}
