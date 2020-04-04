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
		for(int i=0;i<15;i++)
		{

			tree.insert(i, new Ref("asd"));
		}
		tree.insert(3, new Ref("asd"));
		
		tree.toString();
		sc.close();
	}	
}
