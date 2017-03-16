import java.sql.*; 
import oracle.jdbc.pool.OracleDataSource;
import java.io.*; 

class MagicJDBC { 
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
    //-------------Insert tom into MagicCousinBF
    Statement istmt1 = conn.createStatement ();
    istmt1.executeQuery("insert into MCOUSINBF(mcbname) values('tom')");
    istmt1.close();
    //-------------Insert tom into DeltaMagicCousinBF Table
    Statement istmt2 = conn.createStatement ();
    istmt2.executeQuery("insert into DMCOUSINBF(dmcbname) values('tom')");
    istmt2.close();
    //-------------MagicCousinBF TABLE
    MagicCousin:do{
        int i=0;
    Statement stmt = conn.createStatement ();
    ResultSet srset = stmt.executeQuery("select p.pname2 from PARENT p,DMCOUSINBF c where p.pname1=c.dmcbname"); 
    while(srset.next())
    {   
    	String tmcousinsql="INSERT INTO TEMPCOUSINBF (tmcbname) VALUES (?)";
    	PreparedStatement cstmt12=conn.prepareStatement(tmcousinsql);
    	cstmt12.setString(1,srset.getString(1));
    	cstmt12.executeUpdate();
    	cstmt12.close();
    	
    	String mcousinsql="INSERT INTO MCOUSINBF (mcbname) VALUES (?)";
    	PreparedStatement sstmt=conn.prepareStatement(mcousinsql);
    	sstmt.setString(1,srset.getString(1));
    	boolean OldRecord = false;
    	Statement stmt1 = conn.createStatement ();
        ResultSet crset = stmt1.executeQuery("select count(*) from MCOUSINBF where mcbname='"+srset.getString(1)+"'"); 
        crset.next();
        int numofrows=crset.getInt(1);
        OldRecord=(numofrows!=0)?true:false;
    	if(!OldRecord)
    	{
    	i=sstmt.executeUpdate();
    	}
    	stmt1.close();
    	sstmt.close();
    }	
    stmt.close();
    stmt = conn.createStatement ();
    stmt.executeQuery("delete from DMCOUSINBF");
    stmt.close();
    stmt = conn.createStatement ();
    ResultSet crset11 =stmt.executeQuery("select * from TEMPCOUSINBF");
    while(crset11.next())
    {
    	String dmcousinsql="INSERT INTO DMCOUSINBF (dmcbname) VALUES (?)";
    	PreparedStatement cstmt11=conn.prepareStatement(dmcousinsql);
    	cstmt11.setString(1,crset11.getString(1));
    	cstmt11.executeUpdate();
    	cstmt11.close();
    }
    stmt.close();
    stmt = conn.createStatement ();
    stmt.executeQuery("delete from TEMPCOUSINBF");
    stmt.close();
    if(i!=0) continue MagicCousin;
    break MagicCousin;
  }while(true);
    //-----------MagicSiblingBF Table
    Statement stmt = conn.createStatement ();
    ResultSet mrset = stmt.executeQuery("select p.pname2 from PARENT p,MCOUSINBF c where p.pname1=c.mcbname"); 
    while(mrset.next())
    {   	
    	String msiblingsql="INSERT INTO MSIBLINGBF (msbname) VALUES (?)";
    	PreparedStatement mstmt=conn.prepareStatement(msiblingsql);
    	mstmt.setString(1,mrset.getString(1));
    	boolean OldRecord1 = false;
    	Statement mstmt1 = conn.createStatement ();
        ResultSet msrset = mstmt1.executeQuery("select count(*) from MSIBLINGBF where msbname='"+mrset.getString(1)+"'"); 
        msrset.next();
        int snumofrows=msrset.getInt(1);
        OldRecord1=(snumofrows!=0)?true:false;
    	if(!OldRecord1)
    	{
    	mstmt.executeUpdate();
    	}
    	mstmt1.close();
    	mstmt.close();
    }	
    stmt.close();
    //----------SiblingBF Table
    Statement sbstmt = conn.createStatement ();
    ResultSet sbrset = sbstmt.executeQuery("select p1.pname1, p2.pname1 from PARENT p1, PARENT p2,MSIBLINGBF s where p1.pname1=s.msbname and p1.pname2=p2.pname2 and p1.pname1<>p2.pname1");
    while(sbrset.next())
    {   
    	String siblingsql="INSERT INTO SIBLINGBF (sbname1,sbname2) VALUES (?,?)";
    	PreparedStatement sbstmt12=conn.prepareStatement(siblingsql);
    	sbstmt12.setString(1,sbrset.getString(1));
    	sbstmt12.setString(2,sbrset.getString(2));
    	boolean OldRecord2 = false;
    	Statement sstmt2 = conn.createStatement ();
        ResultSet ssrset = sstmt2.executeQuery("select count(*) from SIBLINGBF where sbname1='"+sbrset.getString(1)+"' and sbname2='"+sbrset.getString(2)+"'"); 
        ssrset.next();
        int sbnumofrows=ssrset.getInt(1);
        OldRecord2=(sbnumofrows!=0)?true:false;
    	if(!OldRecord2)
    	{
    	sbstmt12.executeUpdate();
    	}
    	sstmt2.close();
    	sbstmt12.close();
    }
    sbstmt.close();
    //--------------CousinBF Query1 Table
    Statement cbstmt = conn.createStatement ();
    ResultSet cbrset = cbstmt.executeQuery("select p1.pname1, p2.pname1 from PARENT p1, PARENT p2, SIBLINGBF s,MCOUSINBF m where p1.pname2=s.sbname1 and p2.pname2=s.sbname2 and p1.pname1=m.mcbname");
    while(cbrset.next())
    {  
    	String dcousinsql1="INSERT INTO DCOUSINBF (dcbname1,dcbname2) VALUES (?,?)";
    	PreparedStatement dcbstmt12=conn.prepareStatement(dcousinsql1);
    	dcbstmt12.setString(1,cbrset.getString(1));
    	dcbstmt12.setString(2,cbrset.getString(2));
    	boolean OldRecord5 = false;
    	Statement dcstmt2 = conn.createStatement ();
        ResultSet dcsrset = dcstmt2.executeQuery("select count(*) from DCOUSINBF where dcbname1='"+cbrset.getString(1)+"' and dcbname2='"+cbrset.getString(2)+"'"); 
        dcsrset.next();
        int dcbnumofrows=dcsrset.getInt(1);
        OldRecord5=(dcbnumofrows!=0)?true:false;
    	if(!OldRecord5)
    	{
    	dcbstmt12.executeUpdate();
    	}
    	dcstmt2.close();
    	dcbstmt12.close();
    	
    	String cousinsql="INSERT INTO COUSINBF (cbname1,cbname2) VALUES (?,?)";
    	PreparedStatement cbstmt12=conn.prepareStatement(cousinsql);
    	cbstmt12.setString(1,cbrset.getString(1));
    	cbstmt12.setString(2,cbrset.getString(2));
    	boolean OldRecord3 = false;
    	Statement cstmt2 = conn.createStatement ();
        ResultSet csrset = cstmt2.executeQuery("select count(*) from COUSINBF where cbname1='"+cbrset.getString(1)+"' and cbname2='"+cbrset.getString(2)+"'"); 
        csrset.next();
        int cbnumofrows=csrset.getInt(1);
        OldRecord3=(cbnumofrows!=0)?true:false;
    	if(!OldRecord3)
    	{
    	cbstmt12.executeUpdate();
    	}
    	cstmt2.close();
    	cbstmt12.close();
    }
    cbstmt.close();
    
    //--------------CousinBF Query2 Table
    CousinBF:do{
        int k=0;
    Statement istmt3 = conn.createStatement ();
    ResultSet csrset = istmt3.executeQuery("select distinct p1.pname1, p2.pname1 from PARENT p1, PARENT p2, DCOUSINBF c,MCOUSINBF m where p1.pname2=c.dcbname1 and p2.pname2=c.dcbname2 and p1.pname1=m.mcbname"); 
    while(csrset.next())
    {   
    	String tcousinsql="INSERT INTO TCOUSINBF (tcbname1,tcbname2) VALUES (?,?)";
    	PreparedStatement cstmt1=conn.prepareStatement(tcousinsql);
    	cstmt1.setString(1,csrset.getString(1));
    	cstmt1.setString(2,csrset.getString(2));
    	cstmt1.executeUpdate();
    	cstmt1.close();
    	
    	String bcousinsql="INSERT INTO COUSINBF (cbname1,cbname2) VALUES (?,?)";
    	PreparedStatement cbfstmt=conn.prepareStatement(bcousinsql);
    	cbfstmt.setString(1,csrset.getString(1));
    	cbfstmt.setString(2,csrset.getString(2));
    	boolean OldRecord4 = false;
    	Statement cbstmt1 = conn.createStatement ();
        ResultSet ccrset = cbstmt1.executeQuery("select count(*) from COUSINBF where cbname1='"+csrset.getString(1)+"' and cbname2='"+csrset.getString(2)+"'"); 
        ccrset.next();
        int cnumofrows=ccrset.getInt(1);
        OldRecord4=(cnumofrows!=0)?true:false;
    	if(!OldRecord4)
    	{
    	k=cbfstmt.executeUpdate();
    	}
    	cbstmt1.close();
    	cbfstmt.close();
    }	
    istmt3.close();
    stmt = conn.createStatement ();
    stmt.executeQuery("delete from DCOUSINBF");
    stmt.close();
    stmt = conn.createStatement ();
    ResultSet cbrset11 =stmt.executeQuery("select * from TCOUSINBF");
    while(cbrset11.next())
    {
    	String dcousinsql="INSERT INTO DCOUSINBF (dcbname1,dcbname2) VALUES (?,?)";
    	PreparedStatement cbstmt11=conn.prepareStatement(dcousinsql);
    	cbstmt11.setString(1,cbrset11.getString(1));
    	cbstmt11.setString(2,cbrset11.getString(2));
    	cbstmt11.executeUpdate();
    	cbstmt11.close();
    }
    stmt.close();
    stmt = conn.createStatement ();
    stmt.executeQuery("delete from TCOUSINBF");
    stmt.close();
    if(k!=0) continue CousinBF;
    break CousinBF;
  }while(true);
    conn.close();
  }
}