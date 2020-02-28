package kalabalaDB;

import java.io.File;
import java.io.PrintWriter;
import java.util.Hashtable;

public class DBAppTest {
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
	public static void main(String[] args)throws Exception {
		clear();
		tst1();
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
	static void tst1() throws Exception{//TODO:DBAppException
		//Testing Many insertions with many duplicate keys in a random order
//		100 insertions		1 sec
//		500 insertions 		17 sec
//		1000 insertions		54 sec
//		2000 insertions		190 sec
//		5000 insertions		1030 sec
		DBApp dbApp = new DBApp();
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
		for (int i=0;i<200;i++) {
			
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
		System.out.println("Elapsed Time= "+dur+" seconds");
		out.close();
		dbApp.printAllPagesInAllTables();
	}
}
