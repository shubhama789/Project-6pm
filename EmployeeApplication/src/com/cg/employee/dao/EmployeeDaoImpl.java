package com.cg.employee.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.cg.employee.bean.EmployeeBean;
import com.cg.employee.bean.ProjectBean;
import com.cg.employee.exception.EmployeeException;

import com.cg.employee.util.DBConnection;





public class EmployeeDaoImpl implements IEmployeeDao{

	@Override
	public String addProject(ProjectBean project) throws EmployeeException, ClassNotFoundException, IOException, SQLException {
		Connection connection = DBConnection.getConnection();
		PreparedStatement pst = null;
		ResultSet rs = null;
		
		String sequence = "P";
		
		try {
			Statement st = connection.createStatement();
			rs=st.executeQuery("select projectid_sequence.nextval from dual");
			
			while(rs.next()) {
				sequence+=rs.getString(1);
			}
			pst = connection.prepareStatement("insert into projectbean values('"+sequence+"',?,?,?,?,null)");
			
			pst.setString(1,project.getProjectName());
			pst.setString(2,project.getLocation());
			pst.setString(3,project.getProjectStartDate());
			pst.setString(4,project.getProjectEndDate());
			
			pst.executeUpdate();

			return sequence;
		}
		catch(SQLException sqlException)
		{
				//sqlException.printStackTrace();
				throw new EmployeeException("Problem in Inserting Project in database!!");
		}
			
		finally
		{
				try 
				{
					rs.close();
					pst.close();
					connection.close();
				}
				catch (SQLException sqlException) 
				{
					//sqlException.printStackTrace();
					
					throw new EmployeeException("Error in closing db connection");

				}
		}
		
	}

	@Override
	public ProjectBean viewProject(String projectId) throws EmployeeException, ClassNotFoundException, IOException, SQLException {
		Connection connection = DBConnection.getConnection();
		
		Statement st = connection.createStatement();
		ProjectBean projectBean = new ProjectBean();
		ResultSet rs = null;
		try {
		rs = st.executeQuery("select * from projectbean where projectID='"+projectId+"'");
		
		while(rs.next())
		{
		projectBean.setProjectId(rs.getString(1));
		projectBean.setProjectName(rs.getString(2));
		projectBean.setLocation(rs.getString(3));
		projectBean.setProjectStartDate(rs.getString(4));
		projectBean.setProjectEndDate(rs.getString(5));
		projectBean.setNo_of_emp(rs.getInt(6));
		}
		
		return projectBean;
		}
		catch (SQLException e) {
			throw new EmployeeException("Problem in Viewing Project from database!!");
		}
		finally
		{
				try 
				{
					rs.close();
					connection.close();
				}
				catch (SQLException sqlException) 
				{
					//sqlException.printStackTrace();
					
					throw new EmployeeException("Error in closing db connection");

				}
		}
		
	}

	@Override
	public String addEmployee(EmployeeBean employee) throws EmployeeException, ClassNotFoundException, IOException, SQLException{
		Connection connection = DBConnection.getConnection();
		PreparedStatement pst = null;
		ResultSet rs = null;
		
		String sequence = "E";
		
		try {
			Statement st = connection.createStatement();
			rs=st.executeQuery("select empID_sequence.nextval from dual");
			
			while(rs.next()) {
				sequence+=rs.getString(1);
			}
			pst = connection.prepareStatement("insert into employeebean values('"+sequence+"',?,?,?,SYSDATE,?,?,null)");
			
			pst.setString(1,employee.getEmpName());
			pst.setString(2,employee.getPhoneNumber());
			pst.setString(3,employee.getDesignation());
			pst.setDouble(4,employee.getSalary());
			pst.setString(5,employee.getAddress());
			
			pst.executeUpdate();

			return sequence;
		}
		catch(SQLException sqlException)
		{
				//sqlException.printStackTrace();
				throw new EmployeeException("Problem in Inserting EMployee in database!!");
		}

		finally
		{
				try 
				{
					rs.close();
					pst.close();
					connection.close();
				}
				catch (SQLException sqlException) 
				{
					//sqlException.printStackTrace();
					
					throw new EmployeeException("Error in closing db connection");

				}
		}
		
		
	}

	

	@Override
	public EmployeeBean viewEmployeeDetails(String empId) throws EmployeeException, ClassNotFoundException, IOException, SQLException {
		
		Connection connection = DBConnection.getConnection();
		
		Statement st = connection.createStatement();
		EmployeeBean employeeBean = new EmployeeBean();
		ResultSet rs = null;
		try {
		rs = st.executeQuery("select * from employeebean where empID='"+empId+"'");
		
		while(rs.next())
		{
		employeeBean.setEmpId(rs.getString(1));
		employeeBean.setEmpName(rs.getString(2));
		employeeBean.setPhoneNumber(rs.getString(3));
		employeeBean.setDesignation(rs.getString(4));
		employeeBean.setHiredate(rs.getDate(5));
		employeeBean.setSalary(rs.getDouble(6));
		employeeBean.setAddress(rs.getString(7));
		employeeBean.setProjectId(rs.getString(8));
		}
		
		return employeeBean;
		}
		catch (SQLException e) {
			throw new EmployeeException("Problem in Viewing Employee from database!!");
		}
		finally
		{
				try 
				{
					rs.close();
					connection.close();
				}
				catch (SQLException sqlException) 
				{
					//sqlException.printStackTrace();
					
					throw new EmployeeException("Error in closing db connection");

				}
		}
		
	}

	@Override
	public boolean checkAccess(String username, String password) throws EmployeeException, SQLException, ClassNotFoundException, IOException {
		
		Connection connection = DBConnection.getConnection();
		ResultSet rs = null;
		Statement st = connection.createStatement();
		String user=null;
		String pass = null;
		boolean access=false;
		try {
			
			connection.setAutoCommit(false);
			rs=st.executeQuery("select * from admin where username ='"+username+"'");
			
			while(rs.next()) {
				user = rs.getString(1);
				pass = rs.getString(2);
			}
			
			if(user!=null) {
				if(pass.equals(password)) {
					access= true;
				}
				else {
					
					System.out.println("Wrong Password");
					//throw new EmployeeException("wrong paasword");
				}
			}
			else {
				System.out.println("NO such User Exists!!");
			}
			connection.commit();
			return access;
			
		}
		catch (NullPointerException e) {
		
			throw new EmployeeException("There are no records of Admin");
		}
		
		finally
		{
				try 
				{
					rs.close();
					connection.close();
				}
				catch (SQLException sqlException) 
				{
					//sqlException.printStackTrace();
					
					throw new EmployeeException("Error in closing db connection");

				}
		}
			
		
	}

	@Override
	public void removeEmployee(String empID) throws EmployeeException, ClassNotFoundException, IOException, SQLException {
		Connection connection = DBConnection.getConnection();
		Statement st = connection.createStatement();
		PreparedStatement pst1=null;
		String dummy=null;
		String projectId=null;
		ResultSet rs = null;
		try {
		rs=st.executeQuery("select * from employeebean where empid='" + empID + "'");
		while(rs.next()) {
			dummy=rs.getString(1);
			projectId=rs.getString(8);
		}
		
		if(dummy!=null) {
			rs = st.executeQuery("delete from employeebean where empid='" + empID + "'");
			pst1 = connection.prepareStatement("update projectbean set no_of_emp=(select count(*) from employeebean where projectID='"+projectId+"') where projectID='"+projectId+"'");
			
			pst1.executeUpdate();
			
		}else {
			throw new EmployeeException("Employee doesn't exist ");
		}
		}
		catch (SQLException e) {
			throw new EmployeeException("Problem in removing employee from database!!");
		}
		finally
		{
				try 
				{
					rs.close();
					pst1.close();
					connection.close();
				}
				catch (SQLException sqlException) 
				{
					//sqlException.printStackTrace();
					
					throw new EmployeeException("Error in closing db connection");

				}
		}
	}

	@Override
	public void removeProject(String projectID) throws EmployeeException, ClassNotFoundException, IOException, SQLException {
		Connection connection = DBConnection.getConnection();
		Statement st = connection.createStatement();
		String dummy=null;
		ResultSet rs = null;
		try {
		rs=st.executeQuery("select * from projectbean where projectid='" + projectID + "'");
		while(rs.next()) {
			dummy=rs.getString(1);
		}
		
		if(dummy!=null) {
			rs = st.executeQuery("delete from projectbean where projectid='" + projectID + "'");
		}else {
			throw new EmployeeException("Project doesn't exists!!!! ");
		}
		}
		catch (SQLException e) {
			throw new EmployeeException("Problem in Viewing Project from database!!");
		}
		finally
		{
				try 
				{
					rs.close();
					connection.close();
				}
				catch (SQLException sqlException) 
				{
					//sqlException.printStackTrace();
					
					throw new EmployeeException("Error in closing db connection");

				}
		}
	}

	@Override
	public List<EmployeeBean> retriveActive() throws EmployeeException, ClassNotFoundException, IOException, SQLException {
		Connection con= DBConnection.getConnection();
		Statement st=con.createStatement();
		
		List<EmployeeBean> list=null;
		ResultSet rs=null;
		try {
		rs=st.executeQuery("select * from employeebean where projectid IS NOT NULL order by empID");
		list=new ArrayList<>();
		while(rs.next())
		{
			EmployeeBean employeeBean = new EmployeeBean();
			employeeBean.setEmpId(rs.getString(1));
			employeeBean.setEmpName(rs.getString(2));
			employeeBean.setPhoneNumber(rs.getString(3));
			employeeBean.setDesignation(rs.getString(4));
			employeeBean.setHiredate(rs.getDate(5));
			employeeBean.setSalary(rs.getDouble(6));
			employeeBean.setAddress(rs.getString(7));
			employeeBean.setProjectId(rs.getString(8));
			
			//donorId=rs.getString(1);
			list.add(employeeBean);
			
		}
		return list;
		}
		catch (SQLException e) {
			throw new EmployeeException("Problem in Viewing Employee from database!!");
		}
		finally
		{
				try 
				{
					rs.close();
					con.close();
				}
				catch (SQLException sqlException) 
				{
					//sqlException.printStackTrace();
					
					throw new EmployeeException("Error in closing db connection");

				}
		}
	}

	@Override
	public List<EmployeeBean> retriveInActive() throws EmployeeException, SQLException, ClassNotFoundException, IOException {
		Connection con= DBConnection.getConnection();
		Statement st=con.createStatement();
		
		List<EmployeeBean> list=null;
		ResultSet rs=null;
		try {
		rs=st.executeQuery("select * from employeebean where projectid IS NULL order by empID");
		list=new ArrayList<>();
		while(rs.next())
		{
			EmployeeBean employeeBean = new EmployeeBean();
			employeeBean.setEmpId(rs.getString(1));
			employeeBean.setEmpName(rs.getString(2));
			employeeBean.setPhoneNumber(rs.getString(3));
			employeeBean.setDesignation(rs.getString(4));
			employeeBean.setHiredate(rs.getDate(5));
			employeeBean.setSalary(rs.getDouble(6));
			employeeBean.setAddress(rs.getString(7));
			employeeBean.setProjectId(rs.getString(8));
			
			
			list.add(employeeBean);
			
		}
		return list;
		}
		catch (SQLException e) {
			throw new EmployeeException("Problem in Viewing Employee from database!!");
		}
		finally
		{
				try 
				{
					rs.close();
					con.close();
				}
				catch (SQLException sqlException) 
				{
					//sqlException.printStackTrace();
					
					throw new EmployeeException("Error in closing db connection");

				}
		}
	}

	@Override
	public void assignProject(String projectId, String empId) throws EmployeeException, ClassNotFoundException, IOException, SQLException {
		Connection connection = DBConnection.getConnection();
		PreparedStatement pst = null;
		PreparedStatement pst1 = null;
		ResultSet rs = null;
		ResultSet rs1 = null;
		String dummy="";
		boolean dummy2=false;
		try {
			Statement st = connection.createStatement();
			rs=st.executeQuery("select projectid from employeebean where empid='"+empId+"'");
			
			
			while(rs.next()) {
				dummy=rs.getString(1);
			}
			rs1=st.executeQuery("select projectid from projectbean");
			while(rs1.next()) {
				if(projectId.equals(rs1.getString(1))) {
					dummy2=true;
				}
			}
			if(dummy==null) {
				if(dummy2) {
				pst = connection.prepareStatement("update employeebean set projectID='"+projectId+"' where empid='"+empId+"'");
				pst.executeUpdate();
				pst1 = connection.prepareStatement("update projectbean set no_of_emp=(select count(*) from employeebean where projectID='"+projectId+"') where projectID='"+projectId+"'");
				
				pst1.executeUpdate();
				}
				else {
					throw new EmployeeException("Project doesn't exist");
				}
			
			}
			else {
				
				throw new EmployeeException("Project Is already assigned to that Employee");
			}
			
		}
		catch(SQLException sqlException)
		{
				//sqlException.printStackTrace();
				throw new EmployeeException("Problem in Assigning Project to EMployeee in database!!");
		}

		finally
		{
				try 
				{
					rs.close();
					pst.close();
					pst1.close();
					connection.close();
				}
				catch (SQLException sqlException) 
				{
					//sqlException.printStackTrace();
					
					throw new EmployeeException("Error in closing db connection");

				}
		}
		
		
	}

}
