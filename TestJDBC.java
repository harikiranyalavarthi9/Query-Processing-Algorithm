import java.sql.*; 
import oracle.jdbc.pool.OracleDataSource;
import java.io.*; 

class TestJDBC { 
  public static void main (String args []) 
      throws SQLException, IOException { 

    OracleDataSource ods = new OracleDataSource();
    ods.setDriverType("thin");
    ods.setServerName("tinman.cs.gsu.edu");
    ods.setDatabaseName("tinman");
    ods.setPortNumber(new Integer(1522));
    ods.setUser("hyalavarthi1");
    ods.setPassword("hyalavarthi1");

    Connection conn=ods.getConnection();
    //-------------Sibling Table 
    Statement stmt = conn.createStatement (); 
    ResultSet srset = stmt.executeQuery("select p1.pname1, p2.pname1 from PARENT p1, PARENT p2 where p1.pname2=p2.pname2 and p1.pname1 <> p2.pname1"); 
    int i=0;
    while(srset.next())
    {    	
    	String siblingsql="INSERT INTO SIBLING (sname1,sname2) VALUES (?,?)";
    	PreparedStatement sstmt=conn.prepareStatement(siblingsql);
    	sstmt.setString(1,srset.getString(1));
    	sstmt.setString(2,srset.getString(2));
    	i+=sstmt.executeUpdate();
    	sstmt.close();
    }
    System.out.println("Sibling "+i+" records inserted");
    stmt.close();
    System.out.println();
    //---------------Cousin Table and Delta Cousin Table
    stmt = conn.createStatement ();
    ResultSet crset1=stmt.executeQuery("select p1.pname1, p2.pname1 from PARENT p1, PARENT p2, SIBLING s where p1.pname2=s.sname1 and p2.pname2=s.sname2");            
    int cus1=0,dcus1=0;
    while(crset1.next())
    {
    	//String cousin1sql="INSERT INTO COUSIN (cname1,cname2) VALUES ( '"+crset1.getString(1)+"','"+crset1.getString(2)+"')";
    	//String dcousin1sql="INSERT INTO DCOUSIN (dcname1,dcname2) VALUES ( '"+crset1.getString(1)+"','"+crset1.getString(2)+"')";	
    }

    stmt.close();
    //------------------Delta Cousin Query2 Table
    Cousin:do{
    	int itr=0,ditr=0;
    stmt = conn.createStatement ();
    ResultSet crset2=stmt.executeQuery("select p1.pname1, p2.pname1 from PARENT p1, PARENT p2, DCOUSIN c where  p1.pname1=c.dcname1 and p2.pname2=c.dcname2");	
    while(crset2.next())
    {
    	
    	String cousin2sql="INSERT INTO COUSIN (Cname1,Cname2) VALUES (?,?)";
    	PreparedStatement cstmt2=conn.prepareStatement(cousin2sql);
    	cstmt2.setString(1,crset2.getString(1));
    	cstmt2.setString(2,crset2.getString(2));
    	boolean OldRecord = false;
    	Statement stmt1 = conn.createStatement ();
        ResultSet crset = stmt1.executeQuery("select count(*) from COUSIN where cname1='"+crset2.getString(1)+"' and cname2='"+crset2.getString(2)+"'"); 
        crset.next();
        int numofrows=crset.getInt(1);
        OldRecord=(numofrows!=0)?true:false;
    	if(!OldRecord)
    	{
    		itr=cstmt2.executeUpdate();
    		cus1+=itr;
    	}
    	cstmt2.close();
    	stmt1.close();
    	String dcousin2sql="INSERT INTO DCOUSIN (dcname1,dcname2) VALUES (?,?)";
    	PreparedStatement dcstmt2=conn.prepareStatement(dcousin2sql);
    	dcstmt2.setString(1,crset2.getString(1));
    	dcstmt2.setString(2,crset2.getString(2));
    	boolean DOldRecord = false;
    	Statement dstmt1 = conn.createStatement ();
        ResultSet dcrset = dstmt1.executeQuery("select count(*) from DCOUSIN where dcname1='"+crset2.getString(1)+"' and dcname2='"+crset2.getString(2)+"'"); 
        dcrset.next();
        int dnumofrows=dcrset.getInt(1);
        DOldRecord=(dnumofrows!=0)?true:false;
    	if(!DOldRecord)
    	{
    		ditr=dcstmt2.executeUpdate();
    		dcus1+=ditr;
    	}
    	dcstmt2.close();
    	dstmt1.close();
    	
    }
    stmt.close();
    //System.out.println("Cousin Iteration:"+itr+" inserted");
    //System.out.println("Cousin Iteration:"+ditr+" inserted");
    if(ditr!=0) continue Cousin;
    break Cousin;
  }while(true);
    
    System.out.println("Cousin "+cus1+" records inserted");
    System.out.println("Delta Cousin Query2 "+dcus1+" records inserted");
    conn.close();
  } 

} 