package NameNotFound;



import java.awt.Point;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;



public class DBApp {
	
	static class HTSort implements Comparator<Hashtable>{
		Object PK;
		public HTSort(String PK) {
			this.PK=PK;
		}
		
		public int compare(Hashtable a, Hashtable b) {
			String cn=a.get(PK).getClass().getName();
			switch(cn) {
			case "java.lang.Integer":return ((Integer)a.get(PK)).compareTo((Integer)b.get(PK));
			case "java.lang.String":return ((String)a.get(PK)).compareToIgnoreCase((String)b.get(PK));
			case "java.lang.Double":return ((Double)a.get(PK)).compareTo((Double)b.get(PK));
			case "java.lang.Boolean":return ((Boolean)a.get(PK)).compareTo((Boolean)b.get(PK));
			case "java.util.Date":return ((Date)a.get(PK)).compareTo((Date)b.get(PK));
			}
			return 0;
			
		}
		public static int compare2(Hashtable a, Hashtable b,String PK) {
			String cn=a.get(PK).getClass().getName();
			switch(cn) {
			case "java.lang.Integer":return ((Integer)a.get(PK)).compareTo((Integer)b.get(PK));
			case "java.lang.String":return ((String)a.get(PK)).compareTo((String)b.get(PK));
			case "java.lang.Double":return ((Double)a.get(PK)).compareTo((Double)b.get(PK));
			case "java.lang.Boolean":return ((Boolean)a.get(PK)).compareTo((Boolean)b.get(PK));
			case "java.util.Date":return ((Date)a.get(PK)).compareTo((Date)b.get(PK));
			}
			return 0;
			
		}
		
		
		
	}
	
	public static Hashtable<String,String> LoadTable(String strTableName){
		Hashtable<String,String> htblColNameType=new Hashtable<String, String>();
		String PK="";
		try {
			BufferedReader br=new BufferedReader(new FileReader("./Data/metadata.csv"));
			String line;
			
			while((line=br.readLine())!=null) {
				String [] X=line.split(",");
				if(X[0].equals(strTableName)) {
					htblColNameType.put(X[1],X[2]);
					if(X[3].equalsIgnoreCase("TRUE"))
						PK=X[1];
				}
				
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		htblColNameType.put("PrimaryKeyIsHere123456",PK);
		return htblColNameType;
	}
	
	public static void serialize(Vector X,String strTableNamePlusPage) {
		try {
			FileOutputStream fileOut =new FileOutputStream("./Data/"+strTableNamePlusPage);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(X);
			out.close();
			fileOut.close();
         
		} 
		catch (IOException i) {
			i.printStackTrace();
		}
	}
	
	public static Vector DeSerialize(String strTableNamePlusPage) {
		Vector Y=null;
		try {
	         FileInputStream fileIn = new FileInputStream("./Data/"+strTableNamePlusPage);
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         Y = (Vector) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	         
	      } catch (ClassNotFoundException c) {
	         
	         c.printStackTrace();
	         
	      }
		return Y;
	}
	
	public void init() {
		boolean flag=false;
		try {
			
			new File("./"+"Data").mkdirs();
			if(new File("./Data/metadata.csv").exists())
				flag=true;
			FileWriter fileWriter = new FileWriter("./Data/metadata.csv",true);
			if(flag==false)
				fileWriter.append("Table Name, Column Name, Column Type, Key, Indexed \n");
			fileWriter.flush();
	        fileWriter.close();
	        			
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void createTable(String strTableName,String strClusteringKeyColumn,  Hashtable<String,String> htblColNameType )throws DBAppException{
		try {
			BufferedReader br=new BufferedReader(new FileReader("./Data/metadata.csv"));
			String line;
			
			while((line=br.readLine())!=null) {
				String [] X=line.split(",");
				if(X[0].equals(strTableName)) {
					throw new DBAppException("Table("+strTableName+") Already Exists");
				}
				
			}
		}
		catch (IOException e) {
			
		}
		
		
		
		Enumeration<String> keys = htblColNameType.keys();
		
		try {
			FileWriter fileWriter = new FileWriter("./Data/metadata.csv",true);
		
		
		while(keys.hasMoreElements()) {
			String key = keys.nextElement();
			String type=(String) htblColNameType.get(key);
			if(key.equals(strClusteringKeyColumn))
				fileWriter.append(strTableName+","+key+","+type+","+"true"+","+"false"+"\n");
			else
				fileWriter.append(strTableName+","+key+","+type+","+"false"+","+"false"+"\n");
		}
		
		fileWriter.flush();
		fileWriter.close();
		} catch (IOException e) {
			
		}
		
		
		
	}
	
	public void insertIntoTable(String strTableName,Hashtable<String,Object>  htblColNameValue) throws DBAppException {
		Hashtable<String,String> htblColNameType=LoadTable(strTableName);
		String PK=htblColNameType.get("PrimaryKeyIsHere123456");
		htblColNameType.remove("PrimaryKeyIsHere123456");
		Hashtable<String,Object> clone=new Hashtable<String, Object>();
		if(htblColNameValue.get(PK)==null)
			throw new DBAppException("Primary Key ("+PK+") needs to be inserted");
		Enumeration<String> keys = htblColNameValue.keys();
		while(keys.hasMoreElements()) {
			String key=keys.nextElement();
			Object value=htblColNameValue.get(key);
			clone.put(key, value);
			String TableValue=htblColNameType.get(key);
			if(TableValue==null)
				throw new DBAppException(key+" Column Doesnt Exists in Table");
			String classname=value.getClass().getName();
			if(!classname.equals(TableValue))
				throw new DBAppException(key+" Column Value doesnt Match its type");
			
		}
		htblColNameValue.put("TouchDate", new Date());
		int maxpagesize=0;
		Properties property= new Properties();
		try {
			InputStream in=new FileInputStream("./config/DBApp.properties");
			property.load(in);
			maxpagesize=Integer.parseInt(property.getProperty("MaximumRowsCountinPage"));
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		int i=1;
		String rowcount="";
		int pn=0;
		int indexinpage=0;
		boolean done=false;
		boolean done2=false;
		while (true) {
			
			if(!(new File("./Data/"+strTableName+i)).exists()) {
				Vector X=new Vector();
				X.add(htblColNameValue);
				serialize(X, strTableName+i);
				if(done==false) {
					pn=i-1;
					indexinpage=0;
				}
				if(done2==false) {
					rowcount+="b";
				}
				break;
			}
			int z=i+1;
			if((new File("./Data/"+strTableName+z)).exists()) {
				Vector Y=DeSerialize(strTableName+z);
				Hashtable nextpagefirstrow=(Hashtable) Y.get(0);
				if(HTSort.compare2(htblColNameValue, nextpagefirstrow, PK)>0) {
					int s=DeSerialize(strTableName+i).size();
					if(done2==false) {
					for(int s1=0;s1<s;s1++)
						rowcount+="0";
					rowcount+="a";
					}
					i++;
					continue;
				}
					
			}
			
			
			Vector X=DeSerialize(strTableName+i);
			X.add(htblColNameValue);
			Collections.sort(X,new HTSort(PK));
			
			
			if(X.size()>maxpagesize) {
				Hashtable tmp=(Hashtable) X.get(X.size()-1);
				if(tmp==htblColNameValue) {
					if(done2==false) {
						for(int s1=0;s1<maxpagesize;s1++)
							rowcount+="0";
						rowcount+="a";
					}
				}
				else {
					if(done==false) {
						pn=i-1;
						indexinpage=X.indexOf(htblColNameValue);
						done=true;
					}
					if(done2==false) {
						for(int s1=0;s1<X.indexOf(htblColNameValue);s1++)
							rowcount+="0";
						rowcount+="b";
						done2=true;
					}
					htblColNameValue=(Hashtable<String, Object>) X.get(X.size()-1);
				}
					
					
				X.remove(X.size()-1);
				serialize(X,strTableName+i);
				i++;
			}
			else {
				if(done==false) {
					pn=i-1;
					indexinpage=X.indexOf(htblColNameValue);
					done=true;
				}
				if(done2==false) {
					for(int s1=0;s1<X.indexOf(htblColNameValue);s1++)
						rowcount+="0";
					rowcount+="b";
					done2=true;
				}
				serialize(X, strTableName+i);
				break;
			}
			
			
		}
		//System.out.println(pn+" "+indexinpage);
		//System.out.print(rowcount);
		//System.out.println(helper(pn, indexinpage, strTableName));
		HandlebitmapindexInsert(strTableName,rowcount,pn,indexinpage,clone);
		
	}
	
	public void updateTable(String strTableName,String strKey, Hashtable<String,Object> htblColNameValue)  throws DBAppException{
		Hashtable Table=LoadTable(strTableName);
		String PK=(String) Table.get("PrimaryKeyIsHere123456");
		Enumeration<String> keys = htblColNameValue.keys();
		while(keys.hasMoreElements()) {
			String key=keys.nextElement();
			Object value=htblColNameValue.get(key);
			String TableValue=(String) Table.get(key);
			if(TableValue==null)
				throw new DBAppException(key+" Column Doesnt Exists in Table");
			String classname=value.getClass().getName();
			if(!classname.equals(TableValue))
				throw new DBAppException(key+" Column Value doesnt Match its type");
			}
		int i=1;
		int oldpn;
		int oldposinp;
		while (true) {
			if(!(new File("./Data/"+strTableName+i)).exists())
				throw new DBAppException("Row Doesnt Exists");
			Vector X=DeSerialize(strTableName+i);
			for(int j=0;j<X.size();j++) {
				Hashtable Y=(Hashtable) X.get(j);
				if(Y.get(PK).toString().equals(strKey)) {
					oldpn=i-1;
					oldposinp=j;
					Enumeration<String> looper = htblColNameValue.keys();
					boolean upindex=false;
					Hashtable<String,ArrayList<Object>> indexed=new Hashtable<String,ArrayList<Object>>();
					while(looper.hasMoreElements()) {
						String key=looper.nextElement();
						if(new File("./Data/"+strTableName+"IndexOn"+key).exists()) {
							ArrayList<Object> tmp=new ArrayList<Object>();
							tmp.add(0,Y.get(key));
							tmp.add(1,htblColNameValue.get(key));
							indexed.put(key,tmp);
							
						}
						
						if(key.equals(PK)) {
							upindex=true;
							continue;
						}
						Object value=htblColNameValue.get(key);
						Y.put(key, value);
					}
					if(upindex==false) {
						Y.put("TouchDate", new Date());
						serialize(X, strTableName+i);
						Enumeration<String> P = indexed.keys();
						while(P.hasMoreElements()) {
							String key=P.nextElement();
							updatehelper1(strTableName,key, indexed.get(key).get(0), indexed.get(key).get(1), oldpn,oldposinp , false, false);
						}
						return;
					}
					else if(upindex=true &&X.size()>1){
						X.remove(Y);
						Enumeration<String> P = indexed.keys();
						while(P.hasMoreElements()) {
							String key=P.nextElement();
							updatehelper2(strTableName,key,oldpn,oldposinp );
						}
						serialize(X, strTableName+i);
						Y.put(PK, htblColNameValue.get(PK));
						Y.remove("TouchDate");
						insertIntoTable(strTableName, Y);
						return;
					}
					else {
						Y.put(PK,htblColNameValue.get(PK));
						serialize(X, strTableName+i);
						Y.remove("TouchDate");
						deleteFromTable(strTableName, Y);
						insertIntoTable(strTableName, Y);
						return;
					}
					
					
					
				}
			}
			i++;
		}
		
		
	}
	
	public void updatehelper1(String TN,String CN,Object old,Object New,int pn,int indexinpn,boolean found1,boolean found2) {
		int pc=1;
		Entry format=null;
		while(true) {
			if(!new File("./Data/"+TN+"IndexOn"+CN+"/"+pc).exists()) {
				format.value=New;	
				format.RLD();
				for(int i=0;i<format.run.size();i++) {
					format.run.get(i).replaceAll(x ->"0");
				}
				format.run.get(pn).remove(indexinpn);
				format.run.get(pn).add(indexinpn,"1");
				Vector X=new Vector();
				format.RLE();
				X.add(format);
				serialize(X,TN+"IndexOn"+CN+"/"+pc);
				sortdeleted(TN, CN);
				return;
			}
			
			
			Vector X=DeSerialize(TN+"IndexOn"+CN+"/"+pc);
				for(int i=0;i<X.size();i++) {
					Entry k=(Entry)X.get(i);
					
					if(found1==false&&k.value.equals(old)) {
						k.RLD();
						k.run.get(pn).remove(indexinpn);
						k.run.get(pn).add(indexinpn,"0");
						k.RLE();
						found1=true;
						serialize(X,TN+"IndexOn"+CN+"/"+pc);
					}
					if(found2==false&&k.value.equals(New)) {
						k.RLD();
						k.run.get(pn).remove(indexinpn);
						k.run.get(pn).add(indexinpn,"1");
						k.RLE();
						found2=true;
						serialize(X,TN+"IndexOn"+CN+"/"+pc);
					}
					if(pc==1&&i==0)
						format=k;
					if(found1==true&&found2==true) {
						sortdeleted(TN, CN);
						return;}
						
				}
				
			pc++;
		}
		
		
	}
	
	public void updatehelper2(String TN,String CN,int pn,int indexinpn) {
		int pc=1;
		
		while(true) {
			if(!(new File("./Data/"+TN+"IndexOn"+CN+"/"+pc).exists())) {
				sortdeleted(TN, CN);
				return;
			}
			Vector X=DeSerialize(TN+"IndexOn"+CN+"/"+pc);
			for(int i=0;i<X.size();i++) {
				Entry K=(Entry)X.get(0);
				K.RLD();
				K.run.get(pn).remove(indexinpn);
				K.RLE();
			}
			serialize(X, TN+"IndexOn"+CN+"/"+pc);
			pc++;
		}
		
	}
	
	public void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException {
		Hashtable<String,String> htblColNameType=LoadTable(strTableName);
		htblColNameType.remove("PrimaryKeyIsHere123456");
		
		Enumeration<String> keys = htblColNameValue.keys();
		while(keys.hasMoreElements()) {
			String key=keys.nextElement();
			Object value=htblColNameValue.get(key);
			String TableValue=htblColNameType.get(key);
			if(TableValue==null)
				throw new DBAppException(key+" Column Doesnt Exists in Table");
			String classname=value.getClass().getName();
			if(!classname.equals(TableValue))
				throw new DBAppException(key+" Column Value doesnt Match its type");
			
		}
		int negsort=1;
		int i=1;
		String elementssofar="";
		while(true) {
			if(!(new File("./Data/"+strTableName+i)).exists())
				break;
			Vector X=DeSerialize(strTableName+i);
			new File("./Data/"+strTableName+i).delete();
			for(int j=0;j<X.size();j++) {
				Hashtable Y=(Hashtable) X.get(j);
				boolean flag=true;
				keys=htblColNameValue.keys();
				while(keys.hasMoreElements()) {
					String key=keys.nextElement();
					Object Delvalue=htblColNameValue.get(key);
					Object TabValue=Y.get(key);
					if(!(Delvalue.equals(TabValue))) {
						elementssofar+="0";
						flag=false;
						break;
					}
				}
				if(flag) {
					elementssofar+="1";
					X.remove(j);
					j--;
				}
				
			}
			elementssofar+="a";
			if(X.size()!=0)
				serialize(X, strTableName+negsort);
			else {
				negsort--;
			}
				
			negsort++;
			i++;
		}
		System.out.println(elementssofar);
		ArrayList<String> Colnamethathasbmi=new ArrayList<String>();
		try {
			BufferedReader br=new BufferedReader(new FileReader("./Data/metadata.csv"));
			String line;
			
			while((line=br.readLine())!=null) {
				String [] X=line.split(",");
				if(X[0].equals(strTableName)&&X[4].equalsIgnoreCase("true")) {
					Colnamethathasbmi.add(X[1]);
				}
				
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
			System.out.println(elementssofar);
		}
		for(int k=0;k<Colnamethathasbmi.size();k++) {
			String ColName=Colnamethathasbmi.get(k);
			String path="./Data/"+strTableName+"IndexOn"+ColName+"/";
			int pagenumber=1;
			while(true) {
				if(!(new File(path+pagenumber)).exists())
					break;
				Vector X=DeSerialize(strTableName+"IndexOn"+ColName+"/"+pagenumber);
				for(int z=0;z<X.size();z++) {
					int pn=0;
					int index=0;
					Entry elm=(Entry)X.get(z);
					elm.RLD();
					for(int j=0;j<elementssofar.length();j++) {
						if(elementssofar.charAt(j)=='a') {
							pn++;
							index=0;
							
						}
						else if(elementssofar.charAt(j)=='0') {
							index++;
						}
						else {
								elm.run.get(pn).remove(index);
							if(elm.run.get(pn).size()==0) {
								elm.run.remove(pn);
								pn--;
								//could add condition to delete empty bitmapindex
								
							}
						}
					}
					
					elm.RLE();
					serialize(X, strTableName+"IndexOn"+ColName+"/"+pagenumber);
				}				
				pagenumber++;
			}
			sortdeleted( strTableName, ColName);
		}
		
	}
	
	public void sortdeleted(String TN,String CN) {
		String ColType=LoadTable(TN).get(CN);
		int M=0;
		Properties property= new Properties();
		try {
			InputStream in=new FileInputStream("./config/DBApp.properties");
			property.load(in);
			M=Integer.parseInt(property.getProperty("BitmapSize"));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		int pn=1;
		Vector<Entry> all= new Vector<Entry>();
		while(true) {
			if(!(new File("./Data/"+TN+"IndexOn"+CN+"/"+pn)).exists()) {
				break;
			}
			Vector X=DeSerialize(TN+"IndexOn"+CN+"/"+pn);
			new File("./Data/"+TN+"IndexOn"+CN+"/"+pn).delete();
			for(int i=0;i<X.size();i++) {
				Entry k=(Entry)X.get(i);
				k.RLD();
				boolean flag=false;
				for(int j=0;j<k.run.size();j++) {
					for(int m=0;m<k.run.get(j).size();m++)
						if(k.run.get(j).get(m).equalsIgnoreCase("1")) {
							flag=true;
							break;
						}
					if(flag==true)
						break;
				}
				if(flag==true)
					all.add(k);
			}
			
			pn++;
			
		}
		Collections.sort(all,new Comparator<Entry>() {
			public int compare(Entry o1, Entry o2) {
				switch(ColType) {
				case "java.lang.Integer":return ((Integer)o1.value).compareTo((Integer)o2.value);
				case "java.lang.String":return ((String)o1.value).compareToIgnoreCase((String)o2.value);
				case "java.lang.Double":return ((Double)o1.value).compareTo((Double)o2.value);
				case "java.lang.Boolean":return ((Boolean)o1.value).compareTo((Boolean)o2.value);
				case "java.util.Date":return ((Date)o1.value).compareTo((Date)o2.value);
				}
				return 0;
			}
		});
		int pagecount=1;
		Vector tmp=new Vector();
		for(int i=0;i<all.size();i++) {
			if(all.get(i).run.get(all.get(i).run.size()-1).size()==0) {
				all.get(i).run.remove(all.get(i).run.size()-1);
			}					
			all.get(i).RLE();					
			tmp.add(all.get(i));
			if(tmp.size()==M||i==all.size()-1) {
				serialize(tmp,TN+"IndexOn"+CN+"/"+pagecount);
				pagecount++;
				tmp=new Vector();
			}
		}

	}
	
	public void createBitmapIndex(String strTableName,String strColName) throws DBAppException{
		int M=0;
		Properties property= new Properties();
		try {
			InputStream in=new FileInputStream("./config/DBApp.properties");
			property.load(in);
			M=Integer.parseInt(property.getProperty("BitmapSize"));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		try {
		BufferedReader br=new BufferedReader(new FileReader("./Data/metadata.csv"));
		String line;
		ArrayList<String []> old=new ArrayList<String []>();
		while((line=br.readLine())!=null) {
			old.add(line.split(","));
			
		}
		FileWriter fileWriter = new FileWriter("./Data/metadata.csv");
		for(int i=0;i<old.size();i++) {
			if(old.get(i)[0].equals(strTableName)&&old.get(i)[1].equals(strColName)) {
				fileWriter.append(old.get(i)[0]+","+old.get(i)[1]+","+old.get(i)[2]+","+old.get(i)[3]+",TRUE"+"\n");
			}
			else
				fileWriter.append(old.get(i)[0]+","+old.get(i)[1]+","+old.get(i)[2]+","+old.get(i)[3]+","+old.get(i)[4]+"\n");
		}
		
		fileWriter.flush();
		fileWriter.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		
		ArrayList<Entry>indicies=new ArrayList<Entry>();
		new File("./"+"Data/"+strTableName+"IndexOn"+strColName).mkdirs();
		String ColType=LoadTable(strTableName).get(strColName);
		
		int pn=1;
		String rowscount="";
		while (true) {
			if(!(new File("./Data/"+strTableName+pn)).exists()) {
				Collections.sort(indicies,new Comparator<Entry>() {
					public int compare(Entry o1, Entry o2) {
						switch(ColType) {
						case "java.lang.Integer":return ((Integer)o1.value).compareTo((Integer)o2.value);
						case "java.lang.String":return ((String)o1.value).compareToIgnoreCase((String)o2.value);
						case "java.lang.Double":return ((Double)o1.value).compareTo((Double)o2.value);
						case "java.lang.Boolean":return ((Boolean)o1.value).compareTo((Boolean)o2.value);
						case "java.util.Date":return ((Date)o1.value).compareTo((Date)o2.value);
						}
						return 0;
					}
				});
				int pagecount=1;
				Vector tmp=new Vector();
				for(int i=0;i<indicies.size();i++) {
					if(indicies.get(i).run.get(indicies.get(i).run.size()-1).size()==0) {
						indicies.get(i).run.remove(indicies.get(i).run.size()-1);
					}					
					indicies.get(i).RLE();					
					tmp.add(indicies.get(i));
					if(tmp.size()==M||i==indicies.size()-1) {
						serialize(tmp,strTableName+"IndexOn"+strColName+"/"+pagecount);
						pagecount++;
						tmp=new Vector();
					}
				}
				tmp=null;
				indicies=null;
				return;
			}
			Vector X=DBApp.DeSerialize(strTableName+pn);
			for(int i=0;i<X.size();i++) {
				Hashtable Y=(Hashtable) X.get(i);
				Object value=Y.get(strColName);
				
				boolean flag=(value==null)?true:false;
				for(int j=0;j<indicies.size();j++) {
					if(flag==false&&ColType.equalsIgnoreCase("java.lang.String")) {
						if(((String)indicies.get(j).value).equalsIgnoreCase((String)value)) {
							flag=true;
							indicies.get(j).run.get((pn-1)).add(1+"");
							continue;
						}
					}
					
					
					if(flag==false&&indicies.get(j).value.equals(value)) {
						flag=true;
						indicies.get(j).run.get((pn-1)).add(1+"");
						continue;
					}
					indicies.get(j).run.get((pn-1)).add(0+"");
					
				}
				if(flag==false) {
					indicies.add( new Entry(value,rowscount));
				}
				
				rowscount+="1";
			}//first table page end
			pn++;
			for(int k=0;k<indicies.size();k++) {
				indicies.get(k).run.add(new ArrayList<String>());
			}
			rowscount+="a";
		}
		
		
	}
	
	public void HandlebitmapindexInsert(String strTableName,String rowcount,int pn,int indexinpage,Hashtable<String,Object> Value) {
		ArrayList<String> Colnamethathasbmi=new ArrayList<String>();
		try {
			BufferedReader br=new BufferedReader(new FileReader("./Data/metadata.csv"));
			String line;
			
			while((line=br.readLine())!=null) {
				String [] X=line.split(",");
				if(X[0].equals(strTableName)&&X[4].equalsIgnoreCase("true")) {
					Colnamethathasbmi.add(X[1]);
				}
				
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		int M=0;
		int N=0;
		Properties property= new Properties();
		try {
			InputStream in=new FileInputStream("./config/DBApp.properties");
			property.load(in);
			M=Integer.parseInt(property.getProperty("BitmapSize"));
			N=Integer.parseInt(property.getProperty("MaximumRowsCountinPage"));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		for(int i=0;i<Colnamethathasbmi.size();i++) {
			String ColName=Colnamethathasbmi.get(i);
			String path="./Data/"+strTableName+"IndexOn"+ColName+"/";
			String Coltype=LoadTable(strTableName).get(ColName);
			boolean isnull=false;
			if(Value.get(ColName)==null) {
				isnull=true;
			}
			if(!new File(path+1).exists()) {
				if(isnull)
					continue;
				Entry X=new Entry(Value.get(ColName),rowcount);
				X.RLE();
				Vector Y = new Vector();
				Y.add(X);
				serialize(Y,strTableName+"IndexOn"+ColName+"/"+1);
				continue;
			}
			int pagecount=1;
			boolean found=false;
			if(Value.get(ColName)==null) {
				isnull=true;
			}
			while(true) {				
				if(!(new File("./Data/"+strTableName+"IndexOn"+ColName+"/"+pagecount)).exists()) {
					if(found==true||isnull) {
						break;
					}
					Entry Last= new Entry(Value.get(ColName), rowcount+helper(pn, indexinpage, strTableName));
					Last.RLE();
					int pagecount2=1;
					while(true) {
					if(!((new File("./Data/"+strTableName+"IndexOn"+ColName+"/"+pagecount2)).exists())&&Last!=null){
						Vector X=new Vector();
						X.add(Last);
						serialize(X, strTableName+"IndexOn"+ColName+"/"+pagecount2);
						break;
					}
					
					Vector X=DeSerialize(strTableName+"IndexOn"+ColName+"/"+pagecount2);
					X.add(Last);
					Collections.sort(X,new Comparator<Entry>() {
						public int compare(Entry o1, Entry o2) {
							switch(Coltype) {
							case "java.lang.Integer":return ((Integer)o1.value).compareTo((Integer)o2.value);
							case "java.lang.String":return ((String)o1.value).compareToIgnoreCase((String)o2.value);
							case "java.lang.Double":return ((Double)o1.value).compareTo((Double)o2.value);
							case "java.lang.Boolean":return ((Boolean)o1.value).compareTo((Boolean)o2.value);
							case "java.util.Date":return ((Date)o1.value).compareTo((Date)o2.value);
							}
							return 0;
						}
					});
					if(X.size()>M) {
						Last=(Entry) X.get(X.size()-1);
						X.remove(X.size()-1);
						serialize(X, strTableName+"IndexOn"+ColName+"/"+pagecount2);
						pagecount2++;
						continue;
					}
					else {
					serialize(X, strTableName+"IndexOn"+ColName+"/"+pagecount2);
					
					break;
					}
					
					}
					
						break;
				}
				Vector X=DeSerialize(strTableName+"IndexOn"+ColName+"/"+pagecount);
				for(int j=0;j<X.size();j++) {
					Entry bmi=(Entry) X.get(j);
					String insert;
					if(found==false&&bmi.value.equals(Value.get(ColName))) {
						found=true;
						 insert="1";
					}
					else {
						 insert="0";
					}
					
					int pn2=pn;
					int indexinpage2=indexinpage;
					while(true) {
						bmi.RLD();
						if(pn2==bmi.run.size())
							bmi.run.add(new ArrayList<String>());
						bmi.run.get(pn2).add(indexinpage2,insert);
						if(bmi.run.get(pn2).size()>N) {
							insert=bmi.run.get(pn2).get(N);
							bmi.run.get(pn2).remove(N);
							pn2++;
							indexinpage2=0;
							bmi.RLE();
						}
						else {
							bmi.RLE();
							break;
						}
						
							
						
					}
					
					
				}//end for loop vector
				
				serialize(X, strTableName+"IndexOn"+ColName+"/"+pagecount);
				
				pagecount++;
			}//while loop end
			
		}//for loop indicies end
		
	}
	
	public String helper(int pn,int indexinpage,String Tablename) {
		String res="";
		pn++;
		indexinpage++;
		while(true) {
				Vector X=DeSerialize(Tablename+pn);
				for(int i=indexinpage;i<X.size();i++) {
					res+="0";
				}
				pn++;
				indexinpage=0;
				if(!(new File("./Data/"+Tablename+pn)).exists())
					break;
				
				res+="a";
				
		}
		
		
		
		return res;
	}
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[] strarrOperators)  throws DBAppException{
		try {
		ArrayList<ArrayList<ArrayList<String>>> allbmi=new ArrayList<ArrayList<ArrayList<String>>>();
		for(int k=0;k<arrSQLTerms.length;k++) {
			SQLTerm term=arrSQLTerms[k];
			String Coltype=LoadTable(term.strTableName).get(term.strColumnName);
			ArrayList<ArrayList<ArrayList<String>>> eachbmi=new ArrayList<ArrayList<ArrayList<String>>>();
			if(new File("./Data/"+term.strTableName+"IndexOn"+term.strColumnName).exists()) {//indexed
				
				if(term.strOperator.equalsIgnoreCase("<")||term.strOperator.equalsIgnoreCase("<=")||term.strOperator.equalsIgnoreCase("!="))
					searchhelper1(term,eachbmi);
				else
					searchhelper2(term, eachbmi);
			}
			else {
				ArrayList<ArrayList<String>>res=new ArrayList<ArrayList<String>>();
				int pc=1;
				while(true) {
					if(!new File("./Data/"+term.strTableName+pc).exists())
						break;
					res.add(new ArrayList<String>());
					Vector X=DeSerialize(term.strTableName+pc);
					for(int i=0;i<X.size();i++) {
						Object tablevalue=((Hashtable)X.get(i)).get(term.strColumnName);
						int s=compare2(term.objValue,tablevalue, Coltype);
						//System.out.println(term.objValue+" "+tablevalue);
						String insert="";
						switch (term.strOperator) {
						case "<" :insert=(s==1)?"1":"0";break;
						case "<=":insert=(s!=-1&&s!=-3)?"1":"0";break;
						case "=":insert=(s==0)?"1":"0";break;
						case "!=":insert=(s!=0)?"1":"0";break;
						case ">":insert=(s==-1)?"1":"0";break;
						case ">=":insert=(s!=1&& s!=-3)?"1":"0";break;
						default:throw new DBAppException("please enter a valid operator");
						}
						res.get(pc-1).add(insert);
					}
					pc++;
				}
				//System.out.println(res);
				eachbmi.add(res);
				//System.out.println(eachbmi);
			}
			//Start looping on eachbmi and Oring it
			while(eachbmi.size()!=1) {
				//System.out.println(eachbmi+"asd");
				eachbmi.add(applyOP(eachbmi.remove(0),eachbmi.remove(0), "OR"));
			}
			//add result to all bmi
			try {
			allbmi.add(eachbmi.get(0));
			//System.out.println(eachbmi);
			//System.out.println(allbmi);
			}
			catch(Exception e) {
				
			}
			//System.out.println(allbmi);
			
		}
		//start looping on allbmi
		//System.out.println(allbmi);
		if(allbmi.size()>1)
			for(int i=0;i<strarrOperators.length;i++) {
				allbmi.add(applyOP(allbmi.remove(0),allbmi.remove(0),strarrOperators[i]));
			}
		//System.out.println(allbmi);
		//create something that support iterator
		ArrayList<Hashtable<String,Object>> result=new ArrayList<Hashtable<String,Object>>();
		//go get values from table that support or allhbmi.get(0)
		if(!(allbmi.get(0)==null)) {
		for(int i=0;i<allbmi.get(0).size();i++) {
			ArrayList<Integer> indices = new ArrayList<Integer>();
			for(int j=0;j<allbmi.get(0).get(i).size();j++) {
				if(allbmi.get(0).get(i).get(j).equalsIgnoreCase("1")) {
					indices.add(j);
				}
			}
			if(indices.size()!=0) {
				Vector X=DeSerialize(arrSQLTerms[0].strTableName+(i+1));
				for(int j=0;j<indices.size();j++)
					result.add((Hashtable<String, Object>) X.get(indices.get(j)));
			}
		}}
		else {//make message
			
		}
		Iterator itr=result.iterator();
		return itr;
		}catch(Exception e) {
			e.printStackTrace();
			return null;
			//throw new DBAppException("Please Enter Appropiate Parameters");
		}
			
	}
	public ArrayList<ArrayList<String>> applyOP(ArrayList<ArrayList<String>> X,ArrayList<ArrayList<String>>Y,String OP){
		if(X==null) {
			if(OP.equalsIgnoreCase("AND"))
				return null;
			else
				return Y;
		}
		if(Y==null) {
			if(OP.equalsIgnoreCase("AND"))
				return null;
			else
				return X;
		}
		ArrayList<ArrayList<String>> res=new ArrayList<ArrayList<String>>();
		for(int i=0;i<X.size();i++) {
			res.add(new ArrayList<String>());
			for(int j=0;j<X.get(i).size();j++) {
				if(OP.equalsIgnoreCase("AND")) {
					int v1=Integer.parseInt(X.get(i).get(j));
					int v2=Integer.parseInt(Y.get(i).get(j));
					int v3 =v1*v2;
					res.get(i).add(""+v3);
				}
					
				else if(OP.equalsIgnoreCase("OR")) {
					int v1=Integer.parseInt(X.get(i).get(j));
					int v2=Integer.parseInt(Y.get(i).get(j));
					int v3 =(v1==1||v2==1)?1:0;
					res.get(i).add(""+v3);
				}
				
				else {
					int v1=Integer.parseInt(X.get(i).get(j));
					int v2=Integer.parseInt(Y.get(i).get(j));
					int v3 =(v1+v2==0)?0:((v1+v2==1)?1:0);
					res.get(i).add(""+v3);
				}
			}
		}
		
		//System.out.println(res);
		return res;
	}
	
	public void searchhelper1(SQLTerm term,ArrayList<ArrayList<ArrayList<String>>> result) {
		int pc=1;
		String Coltype=LoadTable(term.strTableName).get(term.strColumnName);
		while(true) {
			if(!new File("./Data/"+term.strTableName+"IndexOn"+term.strColumnName+"/"+pc).exists()) {
				return;
			}
			Vector X=DeSerialize(term.strTableName+"IndexOn"+term.strColumnName+"/"+pc);
			for(int i=0;i<X.size();i++) {
				Entry k=(Entry) X.get(i);
				k.RLD();
				if(term.strOperator.equalsIgnoreCase("!=")) {
					if(term.objValue.equals(k.value))
						continue;
					result.add(k.run);
					
				}
				else if(term.strOperator.equalsIgnoreCase("<")) { 
					int s=compare(k, term.objValue, Coltype);
					if(s==1)
						result.add(k.run);
					else
						return;
				}
				else {//<=
					int s=compare(k, term.objValue, Coltype);
						if(s!=-1) 
						result.add(k.run);
					else
						return;
					
				}
			}
						
			pc++;
		}
	}
	
	public void searchhelper2(SQLTerm term,ArrayList<ArrayList<ArrayList<String>>> result) throws DBAppException {
		//int count=new File("./Data/"+term.strTableName+"IndexOn"+term.strColumnName).listFiles().length;
		String Coltype=LoadTable(term.strTableName).get(term.strColumnName);
		ArrayList<Hashtable<String,Object>> Y=BRINONINDEX(term.strTableName, term.strColumnName);
		int startindex=0;
		for(int i=0;i<Y.size();i++) {
			Object min=Y.get(i).get("Min");
			Object max=Y.get(i).get("MAX");
			if(term.strOperator.equalsIgnoreCase("=")) {
				int s1=compare3(term.objValue,min,Coltype);
				int s2=compare3(term.objValue,max,Coltype);
				if(s1+s2==0||s1+s2==1||s1+s2==-1) {
					startindex=i+1;
					break;
				}
				else if(s1+s2==2) {
					continue;
				}
				else { //-2
					break;
				}
				
			}
			if(term.strOperator.equalsIgnoreCase(">=")) {
				int s1=compare3(term.objValue,min,Coltype);
				int s2=compare3(term.objValue,max,Coltype);
				if(s1+s2==0||s1+s2==1||s1+s2==-1||s1+s2==-2) {
					startindex=i+1;
					break;
				}
				else {
					continue;
				}
				
				
				
			}
			else {//>
				int s1=compare3(term.objValue,min,Coltype);
				int s2=compare3(term.objValue,max,Coltype);
				if(s2==-1) {
					startindex=i+1;
					break;
				}
				else if(s1+s2==2){
					continue;
				}
				else
					break;
				
			}
			
		}
		if(startindex==0) {//entry not in table
			result.add(null);
		}
		else
			BS(term.strTableName,term.strColumnName,term.objValue,term.strOperator,result,startindex,Coltype);
	}
	
	public void BS(String TN,String CN,Object value,String OP,ArrayList<ArrayList<ArrayList<String>>> result,int pn,String coltype) throws DBAppException {
		Vector X= DeSerialize(TN+"IndexOn"+CN+"/"+pn);
		int indexinpage=0;
		int low=0;
		int high=X.size()-1;
		while(low<=high) {
			int mid=(low+high)/2;
			Entry bmi=(Entry) X.get(mid);
			Object bmivalue=bmi.value;
			int s1=compare3(value, bmivalue, coltype);
			if(OP.equalsIgnoreCase("=")) {
				if(s1==0) {
					bmi.RLD();
					result.add(bmi.run);
					return;
				}
				else if(s1==1) {
					low=mid+1;
				}
				else {
					high=mid-1;
				}
			}
			else if(OP.equalsIgnoreCase(">=")) {
				if(s1==0) {
					indexinpage=mid;
					break;
				}
				else if(s1==1) {
					
					low=mid+1;
				}
				else {
					indexinpage=mid;
					high=mid-1;
				}
			}
			else {//>
				 if(s1==1||s1==0) {
					
					low=mid+1;
				}
				else {
					indexinpage=mid;
					high=mid-1;
				}
			}
			
		}
		if(OP.equalsIgnoreCase("=")) {
			result.add(null);
			return;
		}
		for(int i=indexinpage;i<X.size();i++) {
			Entry bmi=(Entry) X.get(i);
			bmi.RLD();
			result.add(bmi.run);
		}
		pn++;
		
		while(true) {
				if(!(new File("./Data/"+TN+"IndexOn"+CN+"/"+pn).exists())) {
					return;
				}
				 X=DeSerialize(TN+"IndexOn"+CN+"/"+pn);
				 for(int i=0;i<X.size();i++) {
					 Entry bmi=(Entry) X.get(i);
					 bmi.RLD();
					 result.add(bmi.run);
				 }
				 pn++;
		}
		
	}
	
	public int compare(Entry o1, Object o2,String Coltype) {
		if(o1.equals(o2))
			return 0;
		switch(Coltype) {
		case "java.lang.Integer":return ((Integer)o2).compareTo((Integer)o1.value);
		case "java.lang.String":return ((String)o2).compareToIgnoreCase((String)o1.value);
		case "java.lang.Double":return ((Double)o2).compareTo((Double)o1.value);
		case "java.lang.Boolean":return ((Boolean)o2).compareTo((Boolean)o1.value);
		case "java.util.Date":return ((Date)o2).compareTo((Date)o1.value);
		}
		return 0;
	}
	public int compare2(Object o1, Object o2,String Coltype) {
		if(o1==null&&o2==null)
			return 0;
		if(o1==null || o2==null)
			return -3;
		switch(Coltype) {
		case "java.lang.Integer":return ((Integer)o1).compareTo((Integer)o2);
		case "java.lang.String":return ((String)o1).compareToIgnoreCase((String)o2);
		case "java.lang.Double":return ((Double)o1).compareTo((Double)o2);
		case "java.lang.Boolean":return ((Boolean)o1).compareTo((Boolean)o2);
		case "java.util.Date":return ((Date)o1).compareTo((Date)o2);
		}
		return 0;
	}
	public int compare3(Object o1, Object o2,String Coltype) throws DBAppException {
		if(o1==(null))
			throw new DBAppException("Please enter appropiate parameters");
		switch(Coltype) {
		
		case "java.lang.Integer":return ((Integer)o1).compareTo((Integer)o2);
		case "java.lang.String":return ((String)o1).compareToIgnoreCase((String)o2);
		case "java.lang.Double":return ((Double)o1).compareTo((Double)o2);
		case "java.lang.Boolean":return ((Boolean)o1).compareTo((Boolean)o2);
		case "java.util.Date":return ((Date)o1).compareTo((Date)o2);
		}
		return 0;
	}
	public ArrayList<Hashtable<String,Object>> BRINONINDEX(String TN,String ColName){
		ArrayList<Hashtable<String,Object>>res=new ArrayList<Hashtable<String,Object>>();
		int pc=1;
		while(true) {
			if(!new File("./Data/"+TN+"IndexOn"+ColName+"/"+pc).exists()) {
				break;
			}
			Vector X=DeSerialize(TN+"IndexOn"+ColName+"/"+pc);
			Hashtable Y=new Hashtable();
			Y.put("Min", ((Entry)X.get(0)).value);
			Y.put("MAX",((Entry)X.get(X.size()-1)).value);
			res.add(Y);
			pc++;
		}
		
		
		
		return res;
	}
	
	public static void main(String[] args) throws DBAppException {
		DBApp T= new DBApp();
		T.init();
		String strTableName = "Student"; 
//		Hashtable htblColNameType = new Hashtable( ); 
//		htblColNameType.put("id", "java.lang.Integer"); 
//		htblColNameType.put("name", "java.lang.String"); 
//		htblColNameType.put("gpa", "java.lang.Double"); 
//		T.createTable( strTableName, "id", htblColNameType );
		T.createBitmapIndex(strTableName, "gpa");
//		T.createBitmapIndex(strTableName, "T1");
//		T.createBitmapIndex(strTableName, "T2");
//		Vector X=((Vector) DeSerialize("StudentIndexOnid/1"));
//		X.remove(1);
//		serialize(X, "StudentIndexOnid/1");
//		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id",new Integer(7));
//		htblColNameType.put("name","Ammar");
//		htblColNameType.put("gpa",new Double(2.0));
//		T.insertIntoTable( strTableName, htblColNameType );
		T.deleteFromTable(strTableName, htblColNameType);
//		T.updateTable(strTableName,"1", htblColNameType);
		System.out.println(DeSerialize("Student1"));
		System.out.println(DeSerialize("Student2"));
		System.out.println(DeSerialize("Student3"));
//		System.out.println(DeSerialize("Student4"));
//		System.out.println(DeSerialize("Student5"));
//		T.createBitmapIndex("Student","id");
		System.out.println(DeSerialize("StudentIndexOngpa/1"));
//		System.out.println(DeSerialize("StudentIndexOngpa/2"));
//		System.out.println(DeSerialize("UptestIndexOnT2/2"));
//		T.updatehelper1("Student","id",11,12,0,1);
//		System.out.println(DeSerialize("UptestIndexOnid/1"));
//		System.out.println(DeSerialize("UptestIndexOnid/2"));
//		X.RLE();
//		System.out.println(X);
//		T.sortdeleted("Student","id");
		
//		Entry a=((Entry) X.get(0));
//		a.RLD();
//		Entry b=((Entry) X.get(1));
//		b.RLD();
//		System.out.println(T.applyOP(a.run,null, "XOR"));
//		X.RLE();
//		System.out.println(X);
//		ArrayList<ArrayList<ArrayList<String>>>res =new ArrayList<ArrayList<ArrayList<String>>>();
		SQLTerm[] arrSQLTerms; 
		arrSQLTerms = new SQLTerm[1];
		arrSQLTerms[0]= new SQLTerm();
		arrSQLTerms[0].strTableName =  "Student"; 
		arrSQLTerms[0].strColumnName=  "gpa"; 
		arrSQLTerms[0].strOperator  =  "="; 
		arrSQLTerms[0].objValue     =  0;
//		arrSQLTerms[1]= new SQLTerm();
//		arrSQLTerms[1].strTableName =  "Student"; 
//		arrSQLTerms[1].strColumnName=  "id"; 
//		arrSQLTerms[1].strOperator  =  ">="; 
//		arrSQLTerms[1].objValue     =  5;
//		String [] X =new String [1];
//		X[0]="AND";
		long start=System.currentTimeMillis();
		Iterator asd=T.selectFromTable(arrSQLTerms, null);
		while(asd.hasNext())
			System.out.println(asd.next());
		System.out.println(System.currentTimeMillis()-start+" Without");
//		System.out.println(asd);
//		System.out.println(res);
//		System.out.println(new File("./Data/StudentIndexOnid").listFiles().length);
//		T.BRINONINDEX("Student","id");
//		ArrayList<ArrayList<ArrayList<String>>> result=new ArrayList<ArrayList<ArrayList<String>>>();
//		T.BS("Student","id",4,"=", result,1,"java.lang.Integer");
//		System.out.println(result);
		
		
		
		
		
		
		

		
		
	}

}
