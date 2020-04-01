package kalabalaDB;

import java.awt.List;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Scanner;

import BPTree.BPTree;
import BPTree.Ref;
import RTree.RTree;


public class DBAppTest {

	public static void main(String[] args)throws Exception {
//		clear();
//		tab();
//		transform();
		tabDelete();
	}
	
	
	
	
	static void tab() throws Exception{
		DBApp d = new DBApp();
		d.init();
		Hashtable htblColNameType = new Hashtable<>();
		htblColNameType.put("K", "java.lang.Integer");
		htblColNameType.put("I", "java.lang.Integer");
		d.createTable("T", "K", htblColNameType);
		T_insert(3, 5);
		T_insert(1, 4);
		T_insert(2, 2);
		T_insert(6, 7);
		T_insert(11, 8);
		T_insert(5, 5);
		T_insert(5, 7);
		T_insert(9, 3);
		T_insert(6, 4);
		T_insert(8, 0);
		T_insert(16, 14);
		T_insert(9, 1);
		T_insert(2, 2);
		T_insert(6, 151);
		T_insert(1, 1);
		T_insert(4, 9);
			
	}
	static void T_insert(int k,int i) throws Exception{
		DBApp d = new DBApp();
		d.init();
		Hashtable htblColNameValue = new Hashtable<>();
		htblColNameValue.put("K", k);
		htblColNameValue.put("I", i);
		d.insertIntoTable("T", htblColNameValue);
	}
	static void tabDelete() throws Exception{
		DBApp d = new DBApp();
		d.init();
		T_delete(-1,4);
//		T_insert(1, 4);
//		T_insert(2, 2);
	}
	static void T_delete(int k,int i) throws Exception{
		DBApp d = new DBApp();
		d.init();
		Hashtable htblColNameValue = new Hashtable<>();
		if (k!=-1)
			htblColNameValue.put("K", k);
		if (i!=-1)
			htblColNameValue.put("I", i);
		d.deleteFromTable("T", htblColNameValue);
	}
	
	
	static void tst9() throws DBAppException, IOException{
		String strTableName= "Bol";
		String strClusteringKey = "A";
		Hashtable htblColNameType = new Hashtable<>();
		htblColNameType.put("A", "java.lang.Boolean");
		htblColNameType.put("B", "java.lang.String");
		DBApp dbApp = new DBApp();
		dbApp.init();
//		dbApp.printAllPagesInAllTables("9A");
		dbApp.createTable(strTableName, strClusteringKey, htblColNameType);
		for (int i=0;i<50;i++) {
			boolean A= (int)(Math.random()*2)==0;
			String B = randomAlphaNumeric(7);
			Hashtable h = new Hashtable<>();
			h.put("A", A);
			h.put("B", B);
			dbApp.insertIntoTable(strTableName, h);
		}
		dbApp.printAllPagesInAllTables("9dn");

	}
	static void tsta() throws DBAppException, IOException{
		clear();
		
		
		String strTableName= "A";
		String strClusteringKey = "1";
		Hashtable htblColNameType = new Hashtable<>();
		htblColNameType.put("1", "java.lang.Integer");
		htblColNameType.put("2", "java.lang.String");
		DBApp dbApp = new DBApp();
		dbApp.init();
//		dbApp.printAllPagesInAllTables("AA");
		dbApp.createTable(strTableName, strClusteringKey, htblColNameType);
		for (int i=0;i<15;i++) {
			Hashtable htblColNameValue = new Hashtable<>();
			htblColNameValue.put("1", (int)(Math.random()*16));
			htblColNameValue.put("2", randomAlphaNumeric(4));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
		}
		dbApp.printAllPagesInAllTables("AZ");
	}
	static void tstb() throws DBAppException, IOException{
		
		String strTableName= "B";
		String strClusteringKey = "1";
		Hashtable htblColNameType = new Hashtable<>();
		htblColNameType.put("1", "java.lang.Integer");
		htblColNameType.put("2", "java.lang.Double");
		DBApp dbApp = new DBApp();
		dbApp.init();
//		dbApp.printAllPagesInAllTables("BA");
		dbApp.createTable(strTableName, strClusteringKey, htblColNameType);
		for (int i=0;i<15;i++) {
			Hashtable htblColNameValue = new Hashtable<>();
			htblColNameValue.put("1", (int)(Math.random())*16);
			htblColNameValue.put("2", 20*Math.random());
			dbApp.insertIntoTable(strTableName, htblColNameValue);
		}
		dbApp.printAllPagesInAllTables("BZ");
	}
	

	static void tst8() throws  DBAppException, IOException{
		DBApp d = new DBApp();
		d.init();
		d.printAllPagesInAllTables("tst8-0");
		String strTableName = "booleanTest";
		String strClusteringKey = "C1";
		Hashtable htblColNameType = new Hashtable<>();
		htblColNameType.put("C1", "java.lang.Boolean");
		htblColNameType.put("C2", "java.util.Date");
		htblColNameType.put("C3", "java.lang.String");
		d.createTable(strTableName, strClusteringKey, htblColNameType);
		
		for (int i=0;i<19;i++) {
			boolean C1 = ((int)(Math.random()*10)%2==0);
			Date C2 = randomDate();
			String C3 = randomAlphaNumeric(6);
			Hashtable htblColNameValue= new Hashtable<>();
			htblColNameValue.put("C1", C1);
			htblColNameValue.put("C2", C2);
			htblColNameValue.put("C3", C3);
			d.insertIntoTable(strTableName, htblColNameValue);
		}
		d.printAllPagesInAllTables("tst8-1");
	}
	
	
	static void tst7() throws  DBAppException, IOException{
		DBApp d = new DBApp();
		d.init();
//		d.printAllPagesInAllTables("tst7-0");
		String strTableName = "dateTest";
		String strClusteringKey = "C1";
		Hashtable htblColNameType = new Hashtable<>();
		htblColNameType.put("C1", "java.util.Date");
		htblColNameType.put("C2", "java.util.Date");
		htblColNameType.put("C3", "java.lang.String");
		d.createTable(strTableName, strClusteringKey, htblColNameType);
		for (int i=0;i<19;i++) {
			Date C1 = randomDate();
			Date C2 = randomDate();
			String C3 = randomAlphaNumeric(6);
			Hashtable htblColNameValue= new Hashtable<>();
			htblColNameValue.put("C1", C1);
			htblColNameValue.put("C2", C2);
			htblColNameValue.put("C3", C3);
			d.insertIntoTable(strTableName, htblColNameValue);
		}
		d.printAllPagesInAllTables("tst7-1");
	}
	
	static Date randomDate() throws DBAppException{
		
		String res = "";
		int year = 1940+(int)(Math.random()*150);
		int month = 1+(int)(Math.random()*12);
		int day = (int)(Math.random()*31);
		if (month==2) {
			day = Math.max(day, 28);
		}
		else if (month == 4 || month == 6 || month == 9 || month == 11) {
			day = Math.max(day, 30);
		}
		res = String.format("%04d-%02d-%02d", year,month,day);
		System.out.println(res);
		Date d = DBApp.parseDate(res);
		System.out.println(d.toString());
		return d;
		
		
	}
	
	static void tst6() throws DBAppException, IOException{
		DBApp d = new DBApp();
		d.init();
		d.printAllPagesInAllTables("tst6-0");
		Scanner sc = new Scanner(System.in);
		String s = "";
		int n =9;
		for (int i=1;i<=n;i++) {
			Hashtable h = new Hashtable<>();
			String strClusteringKey = "";
			while ( !(s=sc.next()).toLowerCase().equals("u") ) {
				String coln = s.toUpperCase();
				String value = sc.next();
				
				if (coln.equals("C1")) {
					strClusteringKey=value;
				}
				else {
					if (coln.equals("C2")) {
						String C2 = value;
						h.put("C2", C2);
//						out.println(C2);
					}
					else {
						double C3 = Double.parseDouble(value);
						h.put("C3", C3);
//						out.println("\t"+C3+"\t");
					}
//					h.put(coln, value);
				}
			}
			d.updateTable("T1", strClusteringKey, h);
			d.printAllPagesInAllTables("tst6-"+i);
		}
		d.printAllPagesInAllTables("tst6-"+(n+1));
	}
	static void tst5() throws DBAppException{
		showCurrentState("tst5-st");
		DBApp d = new DBApp();
		d.init();
		Scanner sc = new Scanner(System.in);
		for (int i=0;i<7;i++) {
			String s ="";
			Hashtable h = new Hashtable<>();
			while(!(s=sc.next()).equals("d")&&!s.equals("D")) {
				String col = s.toUpperCase();
				String val = sc.next();
//				System.out.println(col+" "+val);
				if (col.equals("A")) {
					boolean C1 = Boolean.parseBoolean(val);
					h.put("A", C1);
				}
				else if (col.equals("B")) {
					h.put("B", val);
				}
//				else {
//					h.put("C3", Double.parseDouble(val));
//				}
				
			}
			System.out.println("About to delete");
			d.deleteFromTable("Bol", h);
			d.printAllPagesInAllTables("tst5-"+i);
		}
//		Hashtable h = new Hashtable<>();
//		h.put("C1",7);
//		System.out.println("About to delete");
//		d.deleteFromTable("T1", h);
//		showCurrentState("tst5-");
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
//		Scanner sc = new Scanner(System.in);
		PrintWriter out = new PrintWriter("tst4updates.txt");
		for (int i=0;i<18;i++) {
//			int C1 = sc.nextInt();
//			String nxt = sc.next();
			int C1 = (int)(Math.random()*8);
			h = new Hashtable<>();
			out.print(i+":\t"+C1+"\t");
			String C2 = randomAlphaNumeric(6);
			h.put("C2", C2);
			double C3 = Math.random()*999;
			h.put("C3", C3);
//			if (nxt.equals("C2")) {
//				String C2 = sc.next();
//				h.put("C2", C2);
//				out.println(C2);
//			}
//			else {
//				double C3 = sc.nextDouble();
//				h.put("C3", C3);
//				out.println("\t"+C3+"\t");
//			}
			try {
				d.updateTable("T1", ""+C1, h);
			}
			catch(DBAppException e) {
				e.printStackTrace();
				i--;
				continue;
				
			}
				
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
	
	public static void transform() throws Exception{
		File data = new File("data");
		String[] list = data.list();
		File fasa= new File("data/0s/");
		fasa.mkdir();
		for (String f : list) {
			if (f.length()>=5&&f.substring(f.length()-5).equals("class")) {
				FileInputStream fi = new FileInputStream("data/"+f); 
				ObjectInputStream obj = new ObjectInputStream(fi);
				Object o = obj.readObject();
				String nf = f.substring(0,f.length()-5)+"tstr";
				PrintWriter pw = new PrintWriter("data/0s/"+nf);
				pw.print(o.toString());
				pw.close();
			}
				
		}
	}
	
}
