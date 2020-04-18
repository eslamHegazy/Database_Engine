package BPTree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Scanner;

import General.Ref;
import kalabalaDB.DBAppException;

public class TestBPTree {
	public static void main(String[] args) throws Exception{
		/*
		BPTree<Integer> b = new BPTree<Integer>(4);
		int n = 15;
		boolean[] vis = new boolean [n];
		int cntr = 0;
		int timer = 0;
		while (cntr < n) {
			timer++;
			int rand = (int)(Math.random()*n);
			if (vis[rand]) continue;
			vis[rand]=true;
			cntr++;
			b.insert(rand, new Ref("r"+timer));
			System.out.println(b);
			System.out.println("----------------------------------------");
		}
		*/
		
		FileInputStream fileIn = new FileInputStream("data/tree.class");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		BPTree<Integer> b =   (BPTree<Integer>) in.readObject();
		in.close();
		fileIn.close();
		
		System.out.println(b);
		System.out.println("----------------------------------------");
//		b.delete(13, "r42");
//		System.out.println(b);
//		System.out.println("----------------------------------------");
		
		FileOutputStream fileOut = new FileOutputStream("data/tree.class");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(b);
		out.close();
		fileOut.close();
		
	}
	/*
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
*/
}
