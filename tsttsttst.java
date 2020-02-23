package DBV2;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

public class tsttsttst {
	static class P implements Serializable{
		transient Vector<o> vec = new Vector<>();

		public P() {
			this.vec = new Vector<>();
		}
		public String toString() {
			StringBuilder sb = new StringBuilder();
			for (o o1 : vec) {
				sb.append(o1);
				sb.append("\n");
			}
			return sb.toString();
		}
		
	}
	static class o implements Serializable {
		int x;
		int y;
		public o(int x, int y) {
			this.x = x;
			this.y = y;
		}
		public String toString () {
			return "("+x+","+y+")";
		}
		
	}
	public static void main(String[] args) throws Exception{
		P p = new P();
		Vector<o> v = p.vec;
		for (int i=0;i<200;i++) {
			int x = (int) (Math.random()*28172);
			int y = (int) (Math.random()*28172);
			o o1 = new o(x,y);
			v.add(o1);
		}
		FileOutputStream fileOut = new FileOutputStream("ka.txt");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		System.out.println(p);
		out.writeObject(p);
		FileInputStream fileIn = new FileInputStream("ka.txt");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		P p1 = (P) in.readObject();
		System.out.println(p1);
	}
}
