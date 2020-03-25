package BPTree;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import kalabalaDB.DBAppException;

public class TestBPTree {

	public static void main(String[] args) throws DBAppException, IOException 
	{
		BPTree<Integer> tree = new BPTree<Integer>(3);
		Scanner sc = new Scanner(System.in);
		for(int i=0;i<15;i++)
		{
			Ref r = new Ref(5);
			tree.insert(i, r);
		}
		
		tree.toString();
		sc.close();
	}	
}
