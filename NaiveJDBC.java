import java.sql.*; 
import oracle.jdbc.pool.OracleDataSource;
import java.io.*; 

class NaiveJDBC { 
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
    //------------------Sibling Table
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
    //------------------Cousin Query1 Table
    stmt = conn.createStatement ();
    ResultSet crset1=stmt.executeQuery("select p1.pname1, p2.pname1 from PARENT p1, PARENT p2, SIBLING s where p1.pname2=s.sname1 and p2.pname2=s.sname2");            
    int cus1=0;
    while(crset1.next())
    {
    	String cousin1sql="INSERT INTO COUSIN (cname1,cname2) VALUES (?,?)";
    	PreparedStatement cstmt1=conn.prepareStatement(cousin1sql);
    	cstmt1.setString(1,crset1.getString(1));
    	cstmt1.setString(2,crset1.getString(2));
    	cus1+=cstmt1.executeUpdate();
    	cstmt1.close();
    }
    //System.out.println("Cousin Query1 "+cus1+" records inserted");
    stmt.close();
    //System.out.println();
    //------------------Cousin Query2 Table
    Cousin:do{
    	int itr=0;
    stmt = conn.createStatement ();
    ResultSet crset2=stmt.executeQuery("select p1.pname1, p2.pname1 from PARENT p1, PARENT p2, COUSIN c where  p1.pname1=c.cname1 and p2.pname2=c.cname2");	
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
    }
    stmt.close();
    //System.out.println("Cousin Iteration:"+itr+" inserted");
    if(itr!=0) continue Cousin;
    break Cousin;
  }while(true);
    
    System.out.println("Cousin "+cus1+" records inserted");
    //------------------Related Query1 Table
    int k=0;
    System.out.println();
    stmt = conn.createStatement ();
    k+=stmt.executeUpdate("insert into related select s.sname1, s.sname2 from SIBLING s");
    stmt.close();
    //------------------Related Query2 Table
    Related0:do{
    	int itr0=0;
    stmt = conn.createStatement ();
    ResultSet rrset1=stmt.executeQuery("select r.rname1, p.pname1 from RELATED r, PARENT p where  r.rname2=p.pname2");
    while(rrset1.next())
    {
    	String related1sql="INSERT INTO RELATED (rname1,rname2) VALUES (?,?)";
    	PreparedStatement rstmt1=conn.prepareStatement(related1sql);
    	rstmt1.setString(1,rrset1.getString(1));
    	rstmt1.setString(2,rrset1.getString(2));
    	boolean OldRecord0 = false;
    	Statement stmt0 = conn.createStatement ();
        ResultSet rrset0 = stmt0.executeQuery("select count(*) from RELATED where rname1='"+rrset1.getString(1)+"' and rname2='"+rrset1.getString(2)+"'"); 
        rrset0.next();
        int r0numofrows=rrset0.getInt(1);
        OldRecord0=(r0numofrows!=0)?true:false;
    	if(!OldRecord0)
    	{
    	itr0=rstmt1.executeUpdate();
    	k+=itr0;
    	}
    	rstmt1.close();
    	stmt0.close();
    }
    stmt.close();
    if(itr0!=0) continue Related0;
    break Related0;
  }while(true);
    //------------------Related Query3 Table
    Related:do{
    	int itr1=0;
    stmt = conn.createStatement ();
    ResultSet rrset2=stmt.executeQuery("select p.pname1, r.rname2 from RELATED r, PARENT p where  r.rname1=p.pname2");
    while(rrset2.next())
    {
    	String related2sql="INSERT INTO RELATED (rname1,rname2) VALUES (?,?)";
    	PreparedStatement rstmt2=conn.prepareStatement(related2sql);
    	rstmt2.setString(1,rrset2.getString(1));
    	rstmt2.setString(2,rrset2.getString(2));
    	boolean OldRecord = false;
    	Statement stmt1 = conn.createStatement ();
        ResultSet rrset = stmt1.executeQuery("select count(*) from RELATED where rname1='"+rrset2.getString(1)+"' and rname2='"+rrset2.getString(2)+"'"); 
        rrset.next();
        int rnumofrows=rrset.getInt(1);
        OldRecord=(rnumofrows!=0)?true:false;
    	if(!OldRecord)
    	{
    		itr1=rstmt2.executeUpdate();
    		k+=itr1;
    	}
    	rstmt2.close();
    	stmt1.close();
    }
    stmt.close();
    //System.out.println("Related Iteration:"+itr1+" inserted");
    if(itr1!=0) continue Related;
    break Related;
  }while(true);
    System.out.println("Related "+k+" records inserted");
    conn.close();
  } 
}