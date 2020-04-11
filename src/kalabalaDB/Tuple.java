package kalabalaDB;
import java.io.*;
import java.util.*;


public class Tuple implements Serializable {
	private Vector attributes = new Vector(); // Transient?
    
	public boolean equals(Object o) {
		Tuple x=(Tuple)o;
		Tuple y=(Tuple)this;
		return x.attributes.get(attributes.size()-1).equals(y.attributes.get(attributes.size()-1));
	}
	
	public int hashCode() {
		return (int)attributes.get(attributes.size()-1);
	}
	
	public void addAttribute(Object o) {
		attributes.add(o);
	}

	public String toString() {
		String str = "";
		for (int i=0;i<attributes.size() - 1;i++) {//-1 to not show our ID for tuple
			Object y = attributes.get(i);
			if (y == null) {
				System.out.println("null");
			} else {

				str += y.toString()+"\t";
			}
		}
		
		return str+"\n";
	}

	public Vector getAttributes() {
		return attributes;
	}

	public void setAttributes(Vector attributes) {
		this.attributes = attributes;
	}


}