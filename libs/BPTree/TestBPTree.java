package BPTree;

import java.io.IOException;
import java.util.Scanner;

import kalabalaDB.DBAppException;

public class TestBPTree {

	public static void main(String[] args) throws DBAppException, IOException 
	{
		BPTree<Integer> tree = new BPTree<Integer>(3);
		Scanner sc = new Scanner(System.in);
		while(true) 
		{
			int x = sc.nextInt();
			if(x == -1)
				break;
			tree.insert(x, null);
			System.out.println(tree.toString());
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
	}	
}
