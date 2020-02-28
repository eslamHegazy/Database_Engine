package kalabalaDB;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Scanner;
import java.util.Vector;

public class tsttsttst {
		
	public static void main(String[] args) throws Exception{
		Vector<Integer> vector = new Vector<Integer>();
		vector.add(1);
		vector.add(1);
		vector.add(3);
		vector.add(3);
		vector.add(3);
		vector.add(3);
		vector.add(7);
		vector.add(9);
		int lo = 0; int hi = vector.size();
		System.out.println(toString(vector));
		Scanner sc = new Scanner(System.in);
		int key = sc.nextInt();
		int med = lo + (hi-lo+1)/2;
		int ans = -1;
		while (lo<hi) {//last occurence of the key
			med = lo + (hi-lo+1)/2;
			int curVal = vector.get(med);
			if (curVal<key) {
				lo = med+1;
			}
			else if (curVal==key) {
				lo = med+1;
				ans = med;
			}
			else {//curVal>med
				hi = med-1;
			}
		}
		System.out.println("last occurence of the key: "+ans);
		if (ans==-1) {//first tuple greater than key
			lo = 0;
			hi = vector.size()-1;
			ans = -1;
			while (lo<=hi) {
				med = lo + (hi-lo+1)/2;
				int curVal = vector.get(med);
				if (curVal<=key) {
					lo = med+1; 
				}
				else {//curVal>med
					ans = med;
					hi = med-1;
				}
			}
			System.out.println("first greater: "+ans);
			if (ans==-1) {//last smaller			
				ans = vector.size();
			}
		}
		if (ans==vector.size()) {
			vector.add(key);
		}
		else {
			vector.insertElementAt(key, ans);
		}
		
		
		System.out.println(toString(vector));
	}
	static String toString(Vector<Integer> v) {
		StringBuilder sb = new StringBuilder();
		for (int i=0;i<v.size();i++)
			sb.append(v.get(i)+",");
		return sb.toString();
	}
}
