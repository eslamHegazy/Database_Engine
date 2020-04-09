package RTree;

public class PushUp<Polygons extends Comparable<Polygons>> {

	/**
	 * This class is used for push keys up to the inner nodes in case
	 * of splitting at a lower level
	 */
	RTreeNode<Polygons> newNode;
	Comparable<Polygons> key;
	
	public PushUp(RTreeNode<Polygons> newNode, Comparable<Polygons> key)
	{
		this.newNode = newNode;
		this.key = key;
	}
}
