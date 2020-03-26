package BPTree;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import kalabalaDB.DBAppException;

public class TestBPTree {

	public static void main(String[] args) throws DBAppException, IOException 
	{
		BPTree<Integer> tree = new BPTree<Integer>(3);
		Scanner sc = new Scanner(System.in);
		for(int i=0;i<5;i++)
		{
			Ref r = new Ref(5);
			int j = (int)Math.random()+5;
			System.out.print(j+" ");
			tree.insert(j, new Ref(j));
		}
		
		tree.toString();
		sc.close();
	}	
}
