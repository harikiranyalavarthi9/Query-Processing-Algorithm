import java.sql.*; 
import oracle.jdbc.pool.OracleDataSource;
import java.io.*; 

class SimpleJDBC { 
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
    //Sibling Open
    Statement stmt = conn.createStatement ();
    ResultSet rset1 = stmt.executeQuery("select p1.Pname1, p2.Pname1 from PARENT p1, PARENT p2 where p1.Pname2=p2.Pname2 and p1.Pname1 <> p2.Pname1"); 
    int i=0;
    while(rset1.next())
    {    	
    	String siblingsql="INSERT INTO SIBLING (Sname1,Sname2) VALUES (?,?)";
    	PreparedStatement prestmt=conn.prepareStatement(siblingsql);
    	prestmt.setString(1,rset1.getString(2));
    	prestmt.setString(2,rset1.getString(1));
    	boolean Notfound = false;
    	Statement stmt1 = conn.createStatement ();
        ResultSet rset2 = stmt1.executeQuery("select count(*) from sibling where Sname1='"+rset1.getString(1)+"' and Sname2='"+rset1.getString(2)+"'"); 
        rset2.next();
        int numofrows=rset2.getInt(1);
        Notfound=(numofrows!=0)?false:true;
    	if(Notfound)
    	{
    	i+=prestmt.executeUpdate();
    	prestmt.close();
    	}
    }
    System.out.println("SIBLING "+i+" records inserted");
    stmt.close();
    //Sibling Close
    System.out.println();
    //Cousin First Line Open
    stmt = conn.createStatement ();
    ResultSet corset2=stmt.executeQuery("select p1.Pname1, p2.Pname1 from PARENT p1, PARENT p2, SIBLING s where  p1.Pname1=s.Sname1 and p2.Pname2=s.Sname2");
    int c1=0;
    while(corset2.next())
    {
    	String cousinsql="INSERT INTO COUSIN (Cname1,Cname2) VALUES (?,?)";
    	PreparedStatement prestmt1=conn.prepareStatement(cousinsql);
    	prestmt1.setString(1,corset2.getString(1));
    	prestmt1.setString(2,corset2.getString(2));
    	boolean Notfound1 = false;
    	Statement stmt2 = conn.createStatement ();
        ResultSet crset2 = stmt2.executeQuery("select count(*) from cousin where Cname1='"+corset2.getString(2)+"' and Cname2='"+corset2.getString(1)+"'"); 
        crset2.next();
        int cnumofrows=crset2.getInt(1);
        Notfound1=(cnumofrows!=0)?false:true;
    	if(Notfound1)
    	{
    	c1+=prestmt1.executeUpdate();
    	prestmt1.close();
    	}
    }
    stmt.close();
    System.out.println("COUSIN "+c1+" records inserted");
    //Cousin First Line Close
    //Cousin Second Line Open
    stmt = conn.createStatement ();
    ResultSet corset3=stmt.executeQuery("select p1.Pname1, p2.Pname1 from PARENT p1, PARENT p2, COUSIN c where  p1.Pname1=c.Cname1 and p2.Pname2=c.Cname2");
    int c2=0;
    while(corset3.next())
    {
    	String cousin1sql="INSERT INTO COUSIN (Cname1,Cname2) VALUES (?,?)";
    	PreparedStatement prestmt2=conn.prepareStatement(cousin1sql);
    	prestmt2.setString(1,corset3.getString(1));
    	prestmt2.setString(2,corset3.getString(2));
    	boolean Notfound2 = false;
    	Statement stmt3 = conn.createStatement ();
        ResultSet crset3 = stmt3.executeQuery("select count(*) from cousin where Cname1='"+corset3.getString(2)+"' and Cname2='"+corset3.getString(1)+"'"); 
        crset3.next();
        int conumofrows=crset3.getInt(1);
        Notfound2=(conumofrows!=0)?false:true;
    	if(Notfound2)
    	{
    	c2+=prestmt2.executeUpdate();
    	prestmt2.close();
    	}
    }
    System.out.println("COUSIN "+c2+" records inserted");
    stmt.close();
    //Cousin Second Line Close
    //Cousin Iteration1 Open
    /*stmt=conn.createStatement();
    ResultSet irset3=stmt.executeQuery("select p1.Pname1, p2.Pname1 from PARENT p1, PARENT p2, COUSIN c where  p1.Pname1=c.Cname1 and p2.Pname2=c.Cname2");
    int c3=0;
    while(irset3.next())
    {
    	String cousin2sql="INSERT INTO COUSIN (Cname1,Cname2) VALUES (?,?)";
    	PreparedStatement iprestmt2=conn.prepareStatement(cousin2sql);
    	iprestmt2.setString(1,irset3.getString(1));
    	iprestmt2.setString(2,irset3.getString(2));
    	c3+=iprestmt2.executeUpdate();
    	iprestmt2.close();
    }
    System.out.println("COUSIN "+c3+" records inserted");
    stmt.close();
    //Cousin Iteration1 Close
    //Cousin Iteration2 Open
    stmt=conn.createStatement();
    ResultSet itrset3=stmt.executeQuery("select p1.Pname1, p2.Pname1 from PARENT p1, PARENT p2, COUSIN c where  p1.Pname1=c.Cname1 and p2.Pname2=c.Cname2");
    int c4=0;
    while(itrset3.next())
    {
    	String cousin3sql="INSERT INTO COUSIN (Cname1,Cname2) VALUES (?,?)";
    	PreparedStatement itprestmt2=conn.prepareStatement(cousin3sql);
    	itprestmt2.setString(1,itrset3.getString(1));
    	itprestmt2.setString(2,itrset3.getString(2));
    	c4+=itprestmt2.executeUpdate();
    	itprestmt2.close();
    }
    System.out.println("COUSIN "+c4+" records inserted");
    stmt.close();
    int k=0;
    System.out.println();
    stmt = conn.createStatement ();
    k+=stmt.executeUpdate("insert into related select s.Sname1, s.Sname2 from SIBLING s");
    stmt.close();
    //----1st insertion
    
    stmt = conn.createStatement ();
    ResultSet rset5=stmt.executeQuery("select r.Rname1, p.Pname1 from RELATED r, PARENT p where  r.Rname2=p.Pname2");
    
    while(rset5.next())
    {
    	String related1sql="INSERT INTO RELATED (Rname1,Rname2) VALUES (?,?)";
    	PreparedStatement prestmt3=conn.prepareStatement(related1sql);
    	prestmt3.setString(1,rset5.getString(1));
    	prestmt3.setString(2,rset5.getString(2));
    	k+=prestmt3.executeUpdate();
    	prestmt3.close();
    }
    stmt.close();
    stmt = conn.createStatement ();
    ResultSet rset6=stmt.executeQuery("select p.Pname1, r.Rname2 from RELATED r, PARENT p where  r.Rname1=p.Pname2");
    while(rset6.next())
    {
    	String related2sql="INSERT INTO RELATED (Rname1,Rname2) VALUES (?,?)";
    	PreparedStatement prestmt4=conn.prepareStatement(related2sql);
    	prestmt4.setString(1,rset6.getString(1));
    	prestmt4.setString(2,rset6.getString(2));
    	k+=prestmt4.executeUpdate();
    	prestmt4.close();
    }
    System.out.println("RELATED "+k+" records inserted");
    stmt.close();*/
    conn.close();
  } 
  }
