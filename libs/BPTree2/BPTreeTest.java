package BPTree2;

import java.util.Scanner;

import BPTree.BPTree;

public class BPTreeTest {
	public static void main(String[] args) 
	{
		BPlusTree<Integer,Integer> tree = new BPlusTree<Integer,Integer>(4);
		Scanner sc = new Scanner(System.in);
		int i=0;
		while(true) 
		{
			int x = sc.nextInt();
			if(x == -1)
				break;
			tree.insert(x, i);
			System.out.println(tree.toString());
			i++;
		}
		while(true) 
		{
			int x = sc.nextInt();
			if(x == -1)
				break;
			tree.delete(x);
			System.out.println(tree.toString());
		}
		sc.close();
		System.out.println(tree.search(16));
		System.out.println(tree.search(16));
	}
}
