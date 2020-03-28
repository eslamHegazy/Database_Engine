package kalabalaDB;

import java.io.Serializable;
import java.util.Hashtable;

public class HashtableSerializer implements Serializable{
	Object[] keys;
	Object[] values;
	public HashtableSerializer(Hashtable h) {
		int N = h.size();
		keys=new Object[N];
		values=new Object[N];
		int cntr=0;
		for (Object x:h.keySet()) {
			keys[cntr]=x;
			values[cntr++]=h.get(x);
		}
	}
	public Hashtable getHashtable() {
		Hashtable h = new Hashtable<>();
		for (int i=0;i<keys.length;i++)
			h.put(keys[i], values[i]);
		return h;
	}
}
