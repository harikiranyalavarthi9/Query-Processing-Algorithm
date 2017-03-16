import java.sql.*; 
import oracle.jdbc.pool.OracleDataSource;
import java.io.*; 

class SemiNaiveJDBC { 
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
    int i=0,j=0;
    while(srset.next())
    {    	
    	String siblingsql="INSERT INTO SIBLING (sname1,sname2) VALUES (?,?)";
    	PreparedStatement sstmt=conn.prepareStatement(siblingsql);
    	sstmt.setString(1,srset.getString(1));
    	sstmt.setString(2,srset.getString(2));
    	i+=sstmt.executeUpdate();
    	sstmt.close();
    	
    	String dsiblingsql="INSERT INTO DSIBLING (dsname1,dsname2) VALUES (?,?)";
    	PreparedStatement dsstmt=conn.prepareStatement(dsiblingsql);
    	dsstmt.setString(1,srset.getString(1));
    	dsstmt.setString(2,srset.getString(2));
    	j+=dsstmt.executeUpdate();
    	dsstmt.close();
    }
    System.out.println("Sibling "+i+" records inserted");
    System.out.println("Delta Sibling "+j+" records inserted");
    stmt.close();
    System.out.println();
    //---------------Cousin Table and Delta Cousin Table
    stmt = conn.createStatement ();
    ResultSet crset1=stmt.executeQuery("select p1.pname1, p2.pname1 from PARENT p1, PARENT p2, DSIBLING s where p1.pname2=s.dsname1 and p2.pname2=s.dsname2");            
    int cus1=0,dcus1=0;
    while(crset1.next())
    {
    	String cousin1sql="INSERT INTO COUSIN (cname1,cname2) VALUES (?,?)";
    	PreparedStatement cstmt1=conn.prepareStatement(cousin1sql);
    	cstmt1.setString(1,crset1.getString(1));
    	cstmt1.setString(2,crset1.getString(2));
    	cus1+=cstmt1.executeUpdate();
    	cstmt1.close();
 
    	String dcousin1sql="INSERT INTO DCOUSIN (dcname1,dcname2) VALUES (?,?)";
    	PreparedStatement dcstmt1=conn.prepareStatement(dcousin1sql);
    	dcstmt1.setString(1,crset1.getString(1));
    	dcstmt1.setString(2,crset1.getString(2));
    	dcus1+=dcstmt1.executeUpdate();
    	dcstmt1.close();
    }
    System.out.println("Cousin Query1 "+cus1+" records inserted");
    System.out.println("Delta Cousin Query1 "+dcus1+" records inserted");
    stmt.close();
    //------------------Cousin Query2 Table
    Cousin:do{
    	int itr=0;
    stmt = conn.createStatement ();
    ResultSet crset2=stmt.executeQuery("select p1.pname1, p2.pname1 from PARENT p1, PARENT p2, DCOUSIN c where  p1.pname1=c.dcname1 and p2.pname2=c.dcname2");	
    while(crset2.next())
    {    	
    	String cousin3sql="INSERT INTO TEMPCOUSIN (tcname1,tcname2) VALUES (?,?)";
    	PreparedStatement cstmt12=conn.prepareStatement(cousin3sql);
    	cstmt12.setString(1,crset2.getString(1));
    	cstmt12.setString(2,crset2.getString(2));
    	cstmt12.executeUpdate();
    	cstmt12.close();
    	
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
    stmt = conn.createStatement ();
    stmt.executeQuery("delete from DCOUSIN");
    stmt.close();
    stmt = conn.createStatement ();
    ResultSet crset11 =stmt.executeQuery("select * from TEMPCOUSIN");
    while(crset11.next())
    {
    	String cousin3sql="INSERT INTO DCOUSIN (dcname1,dcname2) VALUES (?,?)";
    	PreparedStatement cstmt11=conn.prepareStatement(cousin3sql);
    	cstmt11.setString(1,crset11.getString(1));
    	cstmt11.setString(2,crset11.getString(2));
    	cstmt11.executeUpdate();
    	cstmt11.close();
    }
    stmt.close();
    stmt = conn.createStatement ();
    stmt.executeQuery("delete from TEMPCOUSIN");
    stmt.close();
    if(itr!=0) continue Cousin;
    break Cousin;
  }while(true);
    System.out.println("Cousin Query2 "+dcus1+" records inserted");
    System.out.println("Delta Cousin Query2 "+dcus1+" records inserted");
    //-------------Related Query1 Table
    int k=0,rk=0;
    System.out.println();
    stmt = conn.createStatement ();
    k+=stmt.executeUpdate("insert into related select s.dsname1, s.dsname2 from DSIBLING s");
    rk+=stmt.executeUpdate("insert into drelated select s.dsname1, s.dsname2 from DSIBLING s");
    stmt.close();
    System.out.println("Related Query1 "+k+" records inserted");
    System.out.println("Delta Related Query1 "+rk+" records inserted");
    //-----------Related Query2 Table
    Related0:do{
    	int itr0=0,itr1=0;
    stmt = conn.createStatement ();
    ResultSet rrset1=stmt.executeQuery("select r.drname1, p.pname1 from   DRELATED r, PARENT p where  r.drname2=p.pname2");
    while(rrset1.next())
    {
    	String related1sql="INSERT INTO TEMPRELATED (trname1,trname2) VALUES (?,?)";
    	PreparedStatement rstmt1=conn.prepareStatement(related1sql);
    	rstmt1.setString(1,rrset1.getString(1));
    	rstmt1.setString(2,rrset1.getString(2));
    	itr0=rstmt1.executeUpdate();
    	k+=itr0;
    	rstmt1.close();
    	
    	String drelated1sql="INSERT INTO RELATED (rname1,rname2) VALUES (?,?)";
    	PreparedStatement drstmt1=conn.prepareStatement(drelated1sql);
    	drstmt1.setString(1,rrset1.getString(1));
    	drstmt1.setString(2,rrset1.getString(2));
    	boolean DOldRecord0 = false;
    	Statement dstmt0 = conn.createStatement ();
        ResultSet drrset0 = dstmt0.executeQuery("select count(*) from RELATED where rname1='"+rrset1.getString(1)+"' and rname2='"+rrset1.getString(2)+"'"); 
        drrset0.next();
        int dr0numofrows=drrset0.getInt(1);
        DOldRecord0=(dr0numofrows!=0)?true:false;
    	if(!DOldRecord0)
    	{
    	itr1=drstmt1.executeUpdate();
    	rk+=itr1;
    	}
    	drstmt1.close();
    	dstmt0.close();
    }
    
    stmt.close();
    
    //2nd
    int ditr0=0,ditr1=0;
    stmt = conn.createStatement ();
    ResultSet drrset1=stmt.executeQuery("select p.pname1, r.drname1 from   DRELATED r, PARENT p where r.drname1=p.pname2");
    while(drrset1.next())
    {
    	String d1related1sql="INSERT INTO TEMPRELATED (trname1,trname2) VALUES (?,?)";
    	PreparedStatement d1rstmt1=conn.prepareStatement(d1related1sql);
    	d1rstmt1.setString(1,drrset1.getString(1));
    	d1rstmt1.setString(2,drrset1.getString(2));
    	ditr0=d1rstmt1.executeUpdate();
    	k+=ditr0;
    	d1rstmt1.close();
    	
    	String d2related1sql="INSERT INTO RELATED (rname1,rname2) VALUES (?,?)";
    	PreparedStatement d2rstmt1=conn.prepareStatement(d2related1sql);
    	d2rstmt1.setString(1,drrset1.getString(1));
    	d2rstmt1.setString(2,drrset1.getString(2));
    	boolean D1OldRecord0 = false;
    	Statement d1stmt0 = conn.createStatement ();
        ResultSet d1rrset0 = d1stmt0.executeQuery("select count(*) from RELATED where rname1='"+drrset1.getString(1)+"' and rname2='"+drrset1.getString(2)+"'"); 
        d1rrset0.next();
        int dr10numofrows=d1rrset0.getInt(1);
        D1OldRecord0=(dr10numofrows!=0)?true:false;
    	if(!D1OldRecord0)
    	{
    	ditr1=d2rstmt1.executeUpdate();
    	rk+=ditr1;
    	}
    	d2rstmt1.close();
    	d1stmt0.close();
    }
    
    stmt = conn.createStatement ();
    stmt.executeQuery("delete from DRELATED");
    stmt.close();
    stmt = conn.createStatement ();
    ResultSet dcrset11 =stmt.executeQuery("select * from TEMPRELATED");
    while(dcrset11.next())
    {
    	String related3sql="INSERT INTO DRELATED (drname1,drname2) VALUES (?,?)";
    	PreparedStatement dcstmt11=conn.prepareStatement(related3sql);
    	dcstmt11.setString(1,dcrset11.getString(1));
    	dcstmt11.setString(2,dcrset11.getString(2));
    	dcstmt11.executeUpdate();
    	dcstmt11.close();
    }
    stmt.close();
    stmt = conn.createStatement ();
    stmt.executeQuery("delete from TEMPRELATED");
    stmt.close();
    if((itr0+ditr1)!=0) continue Related0;
    break Related0;
  }while(true);
    System.out.println("Related Query2 "+k+" records inserted");
    System.out.println("Related Query2 "+rk+" records inserted");   
    conn.close();
  } 
}