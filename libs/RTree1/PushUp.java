package RTree1;

public class PushUp<T extends Comparable<T>> {

	/**
	 * This class is used for push keys up to the inner nodes in case
	 * of splitting at a lower level
	 */
	RTreeNode<T> newNode;
	Comparable<T> key;
	
	public PushUp(RTreeNode<T> newNode, Comparable<T> key)
	{
		this.newNode = newNode;
		this.key = key;
	}
}
