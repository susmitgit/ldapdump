package com.qnap.ldap.dump;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LdapDump {

	//url : mssql host,mssql DB name,mssql DB user, mssql DB password, mysql host, mysql db, mysql user, mysql password 
	protected String mssqlHost,mssqlDBname,mssqluser,mssqlpass,mysqlHost,mysqlDBname,mysqluser,mysqlpass,loglevel;
		
	public String getMssqlHost() {
		return mssqlHost;
	}
	public void setMssqlHost(String mssqlHost) {
		this.mssqlHost = mssqlHost;
	}
	public String getMssqlDBname() {
		return mssqlDBname;
	}
	public void setMssqlDBname(String mssqlDBname) {
		this.mssqlDBname = mssqlDBname;
	}
	public String getMssqluser() {
		return mssqluser;
	}
	public void setMssqluser(String mssqluser) {
		this.mssqluser = mssqluser;
	}
	public String getMssqlpass() {
		return mssqlpass;
	}
	public void setMssqlpass(String mssqlpass) {
		this.mssqlpass = mssqlpass;
	}
	public String getMysqlHost() {
		return mysqlHost;
	}
	public void setMysqlHost(String mysqlHost) {
		this.mysqlHost = mysqlHost;
	}
	public String getMysqlDBname() {
		return mysqlDBname;
	}
	public void setMysqlDBname(String mysqlDBname) {
		this.mysqlDBname = mysqlDBname;
	}
	public String getMysqluser() {
		return mysqluser;
	}
	public void setMysqluser(String mysqluser) {
		this.mysqluser = mysqluser;
	}
	public String getMysqlpass() {
		return mysqlpass;
	}
	public void setMysqlpass(String mysqlpass) {
		this.mysqlpass = mysqlpass;
	}
	public String getLoglevel() {
		return loglevel;
	}
	public void setLoglevel(String loglevel) {
		this.loglevel = loglevel;
	}
	public static void main(String[] args) throws Exception {
	
		System.out.println((args.length < 8)?"\n\n\n Parameters provided to run the program is not sufficient \n. Please provide parameters as below:\n "
				+ "\nParam 1 = > [MSSQL HOST] -- Required"
				+ "\nParam 2 = > [MSSQL DB NAME] -- Required"
				+ "\nParam 3 = > [MSSQL DB USER] -- Required"
				+ "\nParam 4 = > [MSSQL DB PASSWORD] -- Required"
				+ "\nParam 5 = > [MySQL HOST] -- Required"
				+ "\nParam 6 = > [MySQL DB NAME] -- Required"
				+ "\nParam 7 = > [MySQL DB USER] -- Required"
				+ "\nParam 8 = > [MySQL DB PASSWORD] -- Required"
				+ "\nParam 9 = > [LOG LEVEL - ON => 1, OFF => 0] -- Optional\n\n":"");
		
		LdapDump ldump=new LdapDump();
		ldump.setMssqlHost(args[0]);
		ldump.setMssqlDBname(args[1]);
		ldump.setMssqluser(args[2]);
		ldump.setMssqlpass(args[3]);
		
		ldump.setMysqlHost(args[4]);
		ldump.setMysqlDBname(args[5]);
		ldump.setMysqluser(args[6]);
		ldump.setMysqlpass(args[7]);
		//System.out.println(args.length);
		ldump.setLoglevel( (args.length == 9 )? args[8].toString():"0" );
		//System.out.println(ldump.getLoglevel());
		
		String url ="jdbc:sqlserver://"+ldump.getMssqlHost()+";databaseName="+ldump.getMssqlDBname();
		Connection conn;
		
		if(("1").equals(ldump.getLoglevel()))
		System.out.println("Connecting MSSQL server with connection string as  [  " + url +" user -> "+ldump.getMssqluser()+"  Password -> "+ldump.getMssqlpass() +"  ]");
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			conn = DriverManager.getConnection(url,ldump.getMssqluser(),ldump.getMssqlpass());
			if(("1").equals(ldump.getLoglevel()))
			System.out.println("Connected");
			Statement sta,st; 
			sta = conn.createStatement();
			st = conn.createStatement();
			String Sql = "select EMAIL_ADDRESS,JOB_NUMBER,EMP_NAME,ENG_NAME,DEPT_NUMBER,SUPERVISOR,DEPT_MANAGER,TOP_MANAGER from V_HELPDESK_EMPLOYEE";
			ResultSet rs = sta.executeQuery(Sql);
			//ResultSet rsCount = st.executeQuery("select count(*) from V_HELPDESK_EMPLOYEE");
			// get the number of rows from the result set
			//rsCount.next();
			//int rowCount = rsCount.getInt(1);
			String sql="";//"TRUNCATE TABLE `import_excel`;";
			sql=sql+"INSERT INTO import_excel(`EMAIL`,`EMPID`,`EMPCNAME`,`EMPENGNAME`,`DERNUMBER`,`SUPERVSR`,`DEPMANGR`,`TOPMANGR`) values";
			//int rowcount = 0;
			//return size;
			if(("1").equals(ldump.getLoglevel()))
			System.out.println(" Getting data from MSSQL .. ");
			while(rs.next()){
				sql=sql+""
						+ "('"+ rs.getString(1)+"',"+ "'"+ rs.getString(2)+"',"+ "'"+ rs.getString(3)+"',"+ "'"+ rs.getString(4)+"',"+ "'"+ rs.getString(5)+"',"
						+ "'"+ rs.getString(6)+"',"+ "'"+ rs.getString(7)+"',"+ "'"+  rs.getString(8)+"'"
						+ "),";			
			}
			sql = sql + "('','','','','','','','');";
			rs.close();
			st.close();
			sta.close();
			conn.close();
			if(("1").equals(ldump.getLoglevel()))
			System.out.println(" MSSQL Data capture completed .. ");
			if(new LdapDump().inserRecord(sql,ldump)){
				if(("1").equals(ldump.getLoglevel()))
					System.out.println("Data dumped in MySQL");
				}else{
					if(("1").equals(ldump.getLoglevel()))
					System.out.println("Data couldn't dumped in MySQL");
				}
		} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public boolean inserRecord(String Sql, LdapDump ldump) throws UnsupportedEncodingException {
		
		String username =ldump.getMysqluser() ;
		String password = ldump.getMysqlpass();
		String url = "jdbc:mysql://"+ldump.getMysqlHost()+":3306/"+ldump.getMysqlDBname()+"?user=" + username
				+ "&password=" + password
				+ "&characterEncoding=utf-8&useUnicode=true";
		if(("1").equals(ldump.getLoglevel()))
		System.out.println("Connecting MySQL database...Connection String used [  "+url+"  ]");

		try {
			Class.forName("com.mysql.jdbc.Driver");
			if(("1").equals(ldump.getLoglevel()))
			System.out.println("MySQL Driver loaded!");
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(
					"MySQL Cannot find the driver in the classpath!", e);
		}
		try (Connection con = DriverManager.getConnection(url)) {
			if(("1").equals(ldump.getLoglevel()))
			System.out.println("MySQL Database connected!");
			Statement st;
			st = con.createStatement();
			st.executeUpdate("TRUNCATE TABLE `import_excel`");
			if(("1").equals(ldump.getLoglevel()))
			System.out.println("Table data Truncated!!");			
			st.executeUpdate(Sql);
			st.close();
			con.close();
			return true;
		} catch (SQLException e) {
			throw new IllegalStateException(
					"MySQL Cannot connect the database!", e);
		}
	}

}
