package BPTree;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import General.Ref;
import kalabalaDB.DBAppException;

public class TestBPTree {

	public static void main(String[] args) throws DBAppException, IOException 
	{
		BPTree<Integer> tree = new BPTree<Integer>(3);
		Scanner sc = new Scanner(System.in);
		for(int i=0;i<15;i++)
		{
			tree.insert(i, new Ref("asd1"));
		}
		tree.insert(3, new Ref("asd2"));
		tree.insert(3, new Ref("asd3"));
		tree.insert(3, new Ref("asd4"));
		tree.insert(3, new Ref("asd5"));
		tree.insert(3, new Ref("asd6"));
		tree.insert(3, new Ref("asd7"));
		tree.insert(3, new Ref("asd8"));
		
		tree.delete(3,"asd4");
		tree.delete(3,"asd5");
		tree.delete(3,"asd6");
		tree.delete(3,"asd7");
		tree.delete(3,"asd8");
		tree.toString();
		sc.close();
	}	
}
