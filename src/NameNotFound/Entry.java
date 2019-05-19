package NameNotFound;

import java.io.Serializable;
import java.util.ArrayList;

public class Entry implements Serializable {
	Object value;
	ArrayList<ArrayList<String>> run;
	
	public Entry(Object value,String count) {
		boolean flag=false;
		this.value=value;
		run=new ArrayList<ArrayList<String>>();
		int pn=0;
		run.add(new ArrayList<String>());
		for(int i=0;i<count.length();i++) {
			if(count.charAt(i)=='a') {
				run.add(new ArrayList<String>());
				pn++;
				continue;
			}
			else if(count.charAt(i)=='b') {
				flag=true;
				run.get(pn).add("1");
			}
			else
				run.get(pn).add("0");
		}
		if(flag==true) {
			if(run.get(run.size()-1).size()==0)
				run.remove(run.size()-1);
			return;
		}
		run.get(pn).add("1");
	}
	public String toString() {
		return value+": "+run;
	}
	public  void RLE() 
    { 
		
		for(int j=0;j<run.size();j++) {
			ArrayList<String> tmp=new ArrayList<String>();
			for (int i = 0; i < run.get(j).size(); i++) { 
				int count = 1; 
				while (i < run.get(j).size() - 1 &&  
                   run.get(j).get(i).equals(run.get(j).get(i + 1))) { 
					count++; 
					i++; 
				}
				tmp.add(count+"");
				tmp.add(run.get(j).get(i));
			} 
			run.remove(j);
			run.add(j, tmp);
			tmp=null;
		}
	}
	
	public void  RLD() {
	for(int j=0;j<run.size();j++) {
		ArrayList<String> result = new ArrayList<String> () ;
	    for (int i = 0; i < run.get(j).size(); i+=2) {
	        int count = Integer.parseInt(run.get(j).get(i));
	        String bit=run.get(j).get(i+1);
	        for(int k=0;k<count;k++) {
	        	result.add(bit);
	        }
	        
      }
        run.remove(j);
        run.add(j,result);
        result=null;
    }
	}
	
	
	
}
