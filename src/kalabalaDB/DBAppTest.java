package kalabalaDB;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Scanner;

public class DBAppTest {
	public static void main(String[] args)throws Exception {
//		clear();
//		tst1(500);
//		tst2();
//		tst1(500);
//		tst3();
//		tst4();
//		tst44();
//		tst4();
		tst5();

	}
	static void tst5() throws DBAppException{
		showCurrentState("tst5-st");
		DBApp d = new DBApp();
		d.init();
		Scanner sc = new Scanner(System.in);
//		for (int i=0;i<7;i++) {
//			String s ="";
//			Hashtable h = new Hashtable<>();
//			while(!(s=sc.next()).equals("d")&&!s.equals("D")) {
//				String col = sc.next().toUpperCase();
//				String val = sc.next();
//				h.put(col, val);
//			}
//			System.out.println("About to delete");
//			d.deleteFromTable("T1", h);
//			showCurrentState("tst5-"+i);
//		}
		Hashtable h = new Hashtable<>();
		h.put("C1","0");
		System.out.println("About to delete");
		d.deleteFromTable("T1", h);
//		showCurrentState("tst5-"+i);
		showCurrentState("tst5-en");
	}
	
	static void showCurrentState() throws DBAppException {
		showCurrentState("neww");
	}
	static void showCurrentState(String name) throws DBAppException{
		DBApp D = new DBApp();
		D.init();
		D.printAllPagesInAllTables(name);
	}
	static void tst44() throws Exception{
		DBApp d = new DBApp();
		d.init();
		d.printAllPagesInAllTables("tst44_st");
		System.out.println();
		Hashtable t = new Hashtable<>();
		t.put("C3", 321.0122121);
		int key = 3;
		d.updateTable("T1", ""+key, t);
		d.printAllPagesInAllTables("tst44_en");
	}
	
	static void tst3() throws Exception{
		DBApp d = new DBApp();
		d.init();
		tst1(d);
		d.printAllPagesInAllTables("tst1done");
		d=new DBApp();
		d.printAllPagesInAllTables("new DBApp");
		
	}
	static void tst4() throws DBAppException, IOException{
		clear();
		DBApp d = new DBApp();
		d.init();
		Hashtable h = new Hashtable<>();
		h.put("C1", "java.lang.Integer");
		h.put("C2", "java.lang.String");
		h.put("C3", "java.lang.Double");
		d.createTable("T1", "C1", h);
		h = new Hashtable<>();
		for (int i=0;i<12;i++) {
			h = new Hashtable<>();
			int C1 = (int)(8*Math.random());
			String C2 = randomAlphaNumeric(1+(int)(Math.random()*7));
			double C3 = Math.random()*20;
			h.put("C1", C1);
			h.put("C2", C2);
			h.put("C3", C3);
			d.insertIntoTable("T1", h);
		}
		d.printAllPagesInAllTables("tst4-1");
		System.out.println();
		Scanner sc = new Scanner(System.in);
		PrintWriter out = new PrintWriter("tst4updates.txt");
		for (int i=0;i<7;i++) {
			int C1 = sc.nextInt();
			String nxt = sc.next();
			h = new Hashtable<>();
			out.print(i+":\t"+C1+"\t");
			if (nxt.equals("C2")) {
				String C2 = sc.next();
				h.put("C2", C2);
				out.println(C2);
			}
			else {
				double C3 = sc.nextDouble();
				h.put("C3", C3);
				out.println("\t"+C3+"\t");
			}
			d.updateTable("T1", ""+C1, h);
			
			d.printAllPagesInAllTables("tst4-2-"+i);
		}
	}
	static void clear() {
		File metadata = new File("data/metadata.csv");
		metadata.delete();
		File data = new File("data");
		String[] pages = data.list();
		for (String p: pages) {
			File pageToDelete = new File("data/"+p);
			pageToDelete.delete();
		}
	}
	private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	public static String randomAlphaNumeric(int count) {
		StringBuilder builder = new StringBuilder();
		while (count-- != 0) {
			int character = (int) (Math.random() * ALPHA_NUMERIC_STRING.length());
			builder.append(ALPHA_NUMERIC_STRING.charAt(character));
		}
		return builder.toString();
	}

	static void tst1() throws Exception{
		DBApp dbApp = new DBApp();
		tst1(150, dbApp);
	}
	static void tst1(DBApp dbApp) throws Exception{
		tst1(150,dbApp);
	}
	static void tst1(int n) throws Exception{
		DBApp dbApp = new DBApp();
		tst1(n, dbApp);
	}
	static void tst1(int n,DBApp dbApp) throws Exception{//TODO:DBAppException
		//Testing Many insertions with many duplicate keys in a random order
//		100 insertions		1 sec
//		500 insertions 		17 sec
//		1000 insertions		54 sec
//		2000 insertions		190 sec
//		5000 insertions		1030 sec
//		DBApp dbApp = new DBApp();
		dbApp.init();
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		String strTableName = "Esso Table";
		htblColNameType.put("the key", "java.lang.Integer");
		htblColNameType.put("garbage", "java.lang.Integer");
		htblColNameType.put("Txt", "java.lang.String");
		htblColNameType.put("float qnt", "java.lang.Double");
		dbApp.createTable(strTableName, "the key", htblColNameType);
		File file = new File("Data/tst1_Insertions.txt");
		PrintWriter out = new PrintWriter(file);
		long star = System.nanoTime();
		for (int i=0;i<n;i++) {
			
			int key = 12345+(int)(Math.random()*55);
			double fl = Math.random()*1000000;
			int grbg = (int)(Math.random()*927112);
			String st = randomAlphaNumeric((int)(Math.random()*15));
			Hashtable insrt = new Hashtable<>();
			insrt.put("the key", new Integer(key));
			insrt.put("garbage", new Integer(grbg));
			insrt.put("Txt", st);
			insrt.put("float qnt",new Double(fl));
			String thi = String.format("i=%d:  %d %d %s %f\n\n",i,key,grbg,st,fl);
			out.print(thi);
			dbApp.insertIntoTable(strTableName, insrt);
			System.out.println(i);
		}
		long end = System.nanoTime();
		long dur =( end-star)/(long)1e9;
		System.out.println(n+" insertions, Elapsed Time= "+dur+" seconds");
		out.close();
		dbApp.printAllPagesInAllTables();
	}
	static void tst2() throws Exception{
		DBApp dbApp = new DBApp();
		String strTableName = "Another Table";
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		String[] labels= {"Esso","Body","Abdallah","Ahmed","Arousi","Fathy","Hela","Bela","nice","not nice","7elwa","balwa","thanks"};
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("poly", "java.awt.Polygon");
		htblColNameType.put("label", "java.lang.String");
		dbApp.createTable(strTableName, "id", htblColNameType);
		File file = new File("Data/tst2_Insertions.txt");
		PrintWriter out = new PrintWriter(file);
		long star = System.nanoTime();
		for (int i=0;i<450;i++) {
			
			int id = 100+(int)(Math.random()*25);
			Polygons p = randomPolygon();
			String st = labels[(int)(Math.random()*labels.length)];
			Hashtable insrt = new Hashtable<>();
			insrt.put("id", new Integer(id));
			insrt.put("poly", p);
			insrt.put("label", st);
			String thi = String.format("i=%d:  %d %s %s\n",i,id,p,st);
			out.print(thi);
			dbApp.insertIntoTable(strTableName, insrt);
			System.out.println(i);
		}
		long end = System.nanoTime();
		long dur =( end-star)/(long)1e9;
		out.println();
		System.out.println("Elapsed Time= "+dur+" seconds");
		out.println("Elapsed Time= "+dur+" seconds");
		out.flush();
		dbApp.printAllPagesInAllTables("tst2_dataAfter");
		
	}

	
	static Polygons randomPolygon(){
		int npoints = 3+(int)(Math.random()*13);
		Polygons p = new Polygons();
		for (int i=0;i<npoints;i++) {
			int x = (int)(Math.random()*60);
			int y = (int)(Math.random()*60);
			p.addPoint(x, y);
		}
		return p;
	}
}
