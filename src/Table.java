import java.io.*;
import java.util.*;

public class Table implements Serializable {
	private  Vector<String> pages= new Vector();
	private Vector<String> 	MinMax  = new Vector<>();
	private transient String tableName;
	private transient String strClusteringKey;
	private transient int primaryPos;
	
	public Vector<String> getPages() {
		return pages;
	}
	public void setPages(Vector<String> pages) {
		this.pages = pages;
	}
	public Vector<String> getMinMax() {
		return MinMax;
	}
	public void setMinMax(Vector<String> minMax) {
		MinMax = minMax;
	}
	public String getStrClusteringKey() {
		return strClusteringKey;
	}
	public void setStrClusteringKey(String strClusteringKey) {
		this.strClusteringKey = strClusteringKey;
	}
	
	
	//	private transient Vector colNames;
	//private transient Vector colTypes;

	public Table() {
		
	}
//	public void addToColNames(String s) {
//		colNames.add(s);
//	}
//	public Vector getColNames() {
//		return colNames;
//	}
//	public void addToColTypes(String s) {
//		colTypes.add(s);
//	}
//	public Vector getColTypes() {
//		return colTypes;
//	}
	public void setTableName(String name) {
		tableName=name;
	}
	public void setPrimaryPos(int pos) {
		primaryPos=pos;
	}
	public int getPrimaryPos() {
		return primaryPos;
	}
	public String getTableName() {
		return tableName;
	}
	public static int whereIsIt(Object o,String min,String max,String before,String after){
		String oo=o.toString();
		if(oo.compareTo(min)>=0&&oo.compareTo(max)<=0
				||before.equals("this is the first page")
				||after.equals("this is the last page")){
			return 0;
		}else{
			if(oo.compareTo(min)<0&&oo.compareTo(before)>0){
				return 1;
			}else{
				if(oo.compareTo(before)<=0){
					return 2;
				}else{
					if(oo.compareTo(max)>0&&oo.compareTo(after)<0){
						return 3;
					}else{
						return 4;
					}
				}
			}
		}
		
	}
	public static Page deserialize(String name) throws IOException, ClassNotFoundException{
		FileInputStream fileIn = new FileInputStream(name+".ser");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Page xx = (Page) in.readObject();
		in.close();
		fileIn.close();
		return xx;
	}
	public static void addInVector(Vector<String> vs,String str ,int n){
		for(int i=n;i<vs.size()-1;i++){
			String c=vs.get(i);
			vs.insertElementAt(str, i);
			str=c;
		}
		vs.insertElementAt(str,vs.size()-1);
	}
	public void addInPage(int curr,Tuple x,Object keyValue){
		String pageName=pages.get(curr);
		try {
			Page p=deserialize(pageName);
			if(p.size()<200){
				p.insertIntoPage(x,primaryPos);
				Tuple t0=p.getTuples().get(0);
				Tuple tn=p.getTuples().get(p.size()-1);
				String k0=(t0.getAttributes().get(primaryPos)).toString();
				String kn=(tn.getAttributes().get(primaryPos)).toString();
				String minMax=k0+","+kn;
				MinMax.remove(curr);
				addInVector(MinMax, minMax, curr);
			}else{
				Page n=new Page();
				String pnName=n.getPageName();
				//put from index 100 to 200 in it while figuring out where new tuple will go
				//update the max of the old page and set name MinMax of the new page and put it 
				//in its correct place in the vector
				Tuple t100=p.getTuples().get(100);

				String k100=(t100.getAttributes().get(primaryPos)).toString();

				String newK=keyValue.toString();
				String minMaxO,minMaxN;
				
				Vector<Tuple> nt=new Vector<Tuple>();
				for(int i=100;i<200;i++){
					nt.addElement(p.getTuples().get(i));
				}
				n.setTuples(nt);
				for(int i=199;i<=100;i--){
					nt.remove(i);
				}
				
				//adding the new tuple							
				if(newK.compareTo(k100)<0){
					//add in old page
					p.insertIntoPage(x,primaryPos);
					Tuple t0o=p.getTuples().get(0);
					Tuple t100o=p.getTuples().get(100);
					Tuple t0n=n.getTuples().get(0);
					Tuple t99n=n.getTuples().get(99);
					String k0o=(t0o.getAttributes().get(primaryPos)).toString();
					String k100o=(t100o.getAttributes().get(primaryPos)).toString();
					String k0n=(t0n.getAttributes().get(primaryPos)).toString();
					String k99n=(t99n.getAttributes().get(primaryPos)).toString();
					minMaxO=k0o+","+k100o;
					minMaxN=k0n+","+k99n;
					
				}else{
					//add in new page
					n.insertIntoPage(x,primaryPos);
					Tuple t0o=p.getTuples().get(0);
					Tuple t99o=p.getTuples().get(99);
					Tuple t0n=n.getTuples().get(0);
					Tuple t100n=n.getTuples().get(100);
					String k0o=(t0o.getAttributes().get(primaryPos)).toString();
					String k99o=(t99o.getAttributes().get(primaryPos)).toString();
					String k0n=(t0n.getAttributes().get(primaryPos)).toString();
					String k100n=(t100n.getAttributes().get(primaryPos)).toString();
					minMaxO=k0o+","+k99o;
					minMaxN=k0n+","+k100n;
				}
				addInVector(pages,pnName,curr+1);
				MinMax.remove(curr);
				addInVector(MinMax,minMaxO,curr);
				addInVector(MinMax,minMaxN,curr+1);
			
				n.serialize();
			}
			p.serialize();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
    public void insertSorted(Tuple x,Object keyValue) {
    	int lower=0;
    	int upper=MinMax.size();
    	while(true){
    		//kont mafrod a7ot el condition dah gowa el while ana 3aref bas mesh 3aref 3amalt keda leh we mekasel ashofha
    		if(lower<upper){
    			int curr=(lower+upper)/2;
    			String mM=MinMax.get(curr);
    			String before="this is the first page";
    			String after="this is the last page";
    			String[]inter;
    			if(curr>0){
    				before=MinMax.get(curr-1);
    				inter=before.split(",");
    				before=inter[1];
    			}
    			if(curr<MinMax.size()-1){
    				after=MinMax.get(curr+1);
    				inter=after.split(",");
    				after=inter[0];
    			}
    			//add in which page and new lower and upper
    			String[]MM=mM.split(",");
    			if(whereIsIt(keyValue,MM[0],MM[1],before,after)==0){
    				addInPage(curr,x,keyValue);
    				return;
    			}else{
    				if(whereIsIt(keyValue,MM[0],MM[1],before,after)==1){
    					addInPage(curr-1,x,keyValue);
    					return;
    				}else{
    					if(whereIsIt(keyValue,MM[0],MM[1],before,after)==2){
    						upper=curr;
    					}else{
    						if(whereIsIt(keyValue,MM[0],MM[1],before,after)==3){
    							addInPage(curr,x,keyValue);
    							return;
    						}else{
    							lower=curr;
    						}
    					}
    				}
    			}
    		}else{
    			if(MinMax.size()==0){
    				Page n=new Page();
    				String pnName=n.getPageName();
    				n.insertIntoPage(x,0);
    				pages.addElement(pnName);
    				String key=x.getAttributes().get(primaryPos).toString();
    				String mm=key+","+key;
    				MinMax.addElement(mm);
    				try {
						n.serialize();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
    			}
    		}
    	}
    }
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Page c = new Page();
		Tuple t=new Tuple();
		t.addAttribute("abdallah");
		t.addAttribute(2000);
		t.addAttribute(false);
       // c.addTuple(t);
		Table xxx = new Table();
		//xxx.pages.add(c.getPageName());

		FileOutputStream fileOut = new FileOutputStream("childSer.ser");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(xxx);
		out.close();
		fileOut.close();
		System.out.printf("Serialized data is done");

		FileInputStream fileIn = new FileInputStream("childSer.ser");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Object xx = in.readObject();
		in.close();
		fileIn.close();
		System.out.println(xxx);

	}
}
