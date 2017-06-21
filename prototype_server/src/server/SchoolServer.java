package server;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import ocsf.*;

public class SchoolServer extends AbstractServer
{

	final public static int DEFAULT_PORT = 5556;
	ArrayList<String> arr;

	public SchoolServer(int port)
	{
		super(port);
	}

	public void handleMessageFromClient(Object msg, ConnectionToClient client)
	{
		/************************************************ Checks *************************************************/
		//System.out.println("Request received from " + client);
		Object response = null;
		if (!(msg instanceof ArrayList<?>) || ((ArrayList<String>) msg).size() < 3)
		{
			try
			{
				client.sendToClient(null);
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			return;
		}

		/************************************************ Query handler ******************************************/
		// msg is array list and has at least 2 strings (first for 
		arr = (ArrayList<String>) msg;
		String clientId = arr.remove(0);
		String query = arr.remove(0);

		if (query.equals("select"))
		{
			response = select(arr);
		}
		else if (query.equals("update"))
		{
			response = update(arr);
		}
		else if (query.equals("insert"))
		{
			response = insert(arr);
		}
		else if (query.equals("delete"))
		{
			response = delete(arr);
		}
		else if (query.equals("select field"))
		{
			response = selectField(arr);		
		}
				

		/************************************************ Send to Client ******************************************/
		try
		{
			if (response != null)
				((ArrayList<String>) response).add(0, clientId);
			client.sendToClient(response);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	protected void serverStarted()
	{
		System.out.println("Server listening for connections on port " + getPort());
	}

	protected void serverStopped()
	{
		System.out.println("Server has stopped listening for connections.");
	}

	protected Object selectField(ArrayList<String> arr)
	{
		Statement stmt;
		String sql = "";
		ArrayList<String> answer = new ArrayList<>();
		try
		{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		}
		catch (Exception ex)
		{
			System.out.println("Error - connection to DB");
		}
		try
		{
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0)
			{
				// error handling
				return null;
			}

			sql = "SELECT  " + arr.get(0) + " FROM " + arr.get(1);
			
			if (arr.size() > 1)
			{
				sql += " WHERE ";
				for (int i = 1; i < arr.size(); i += 2)
				{
					sql += arr.get(i) + "=\"" + arr.get(i + 1) + "\" ";
					if (i + 2 < arr.size())
						sql += "AND ";
					
				}
			}
			sql += ";";
			System.out.println("\nSQL: " + sql + "\n");
			ResultSet rs = stmt.executeQuery(sql);
			// need to change "is Logged" field!!!

			ResultSetMetaData metaData = rs.getMetaData();
			int count = metaData.getColumnCount(); // number of column

			while (rs.next())
			{
				String row = "";
				for (int i = 1; i <= count; i++)
				{
					row += metaData.getColumnLabel(i) + "=" + rs.getString(i) + ";";
				}
				if (row.endsWith(";"))
					row = row.substring(0, row.length() - 1);
				answer.add(row);
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return answer;
	}

	protected Object select(ArrayList<String> arr)
	{
		Statement stmt;
		String sql = "";
		ArrayList<String> answer = new ArrayList<>();
		try
		{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		}
		catch (Exception ex)
		{
			System.out.println("Error - connection to DB");
		}
		try
		{
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0)
			{
				// error handling
				return null;
			}

			sql = "SELECT * FROM " + arr.get(0);
			if (arr.size() > 1)
			{
				sql += " WHERE ";
				for (int i = 1; i < arr.size(); i += 2)
				{
					sql += arr.get(i) + "=\"" + arr.get(i + 1) + "\" ";
					if (i + 2 < arr.size())
						sql += "AND ";
					
				}
			}
			sql += ";";
			System.out.println("\nSQL: " + sql + "\n");
			ResultSet rs = stmt.executeQuery(sql);
			// need to change "is Logged" field!!!

			ResultSetMetaData metaData = rs.getMetaData();
			int count = metaData.getColumnCount(); // number of column

			while (rs.next())
			{
				String row = "";
				for (int i = 1; i <= count; i++)
				{
					row += metaData.getColumnLabel(i) + "=" + rs.getString(i) + ";";
				}
				if (row.endsWith(";"))
					row = row.substring(0, row.length() - 1);
				answer.add(row);
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return answer;
	}

	protected Object update(ArrayList<String> arr)
	{
		Statement stmt;
		String sql = "";
		int index = 0;
		ArrayList<String> answer = new ArrayList<>();
		try
		{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		}
		catch (Exception ex)
		{
			System.out.println("Error - connection to DB");
		}
		try
		{
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0)
			{
				// error handling
				return null;
			}

			sql = "UPDATE " + arr.get(0);
			if (arr.size() >= 6)
			{
				sql += " SET ";
				for (int i = 1; i < arr.size(); i += 2)
				{
					if (arr.get(i).equals("conditions"))
					{
						index = i + 1;
						break;
					}
					else
					{
						sql += arr.get(i) + "=\"" + arr.get(i + 1) + "\" ";
						if (i + 2 < arr.size())
							sql += ", ";
					}
				}
				if (sql.endsWith(", "))
					sql = sql.substring(0, sql.length() - 2);
				if (index != 0)
				{
					sql += " WHERE ";
					for (int i = index; i < arr.size(); i += 2)
					{
						sql += arr.get(i) + "=\"" + arr.get(i + 1) + "\" ";
						if (i + 2 < arr.size())
							sql += "AND ";
					}
				}
				else
				{
					System.out.println("Error - No Condition for WHERE");
					return null;
				}

			}
			sql += ";";
			System.out.println("\nSQL: " + sql + "\n");
			int rs = stmt.executeUpdate(sql);
			answer.add("" + rs);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return answer;
	}

	protected Object delete(ArrayList<String> arr)
	{
		Statement stmt;
		String sql = "";
		ArrayList<String> answer = new ArrayList<>();
		try
		{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		}
		catch (Exception ex)
		{
			System.out.println("Error - connection to DB");
		}
		try
		{
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0)
			{
				// error handling
				return null;
			}

			sql = "DELETE FROM " + arr.get(0);
			if (arr.size() >= 3)
			{
				sql += " WHERE ";
				for (int i = 1; i < arr.size(); i += 2)
				{
					sql += arr.get(i) + "=\"" + arr.get(i + 1) + "\" ";
					if (i + 2 < arr.size())
						sql += "AND ";
				}
			}
			sql += ";";
			System.out.println("\nSQL: " + sql + "\n");
			int rs = stmt.executeUpdate(sql);
			answer.add("" + rs);

		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return answer;
	}

	protected Object insert(ArrayList<String> arr)
	{
		Statement stmt;
		String sql = "";
		ArrayList<String> answer = new ArrayList<>();
		int index = 0;
		try
		{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		}
		catch (Exception ex)
		{
			System.out.println("Error - connection to DB");
		}
		try
		{
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0)
			{
				// error handling
				return null;
			}

			sql = "INSERT INTO " + arr.get(0);
			if (arr.size() >= 4)
			{
				sql += " (";
				for (int i = 1; i < arr.size(); i++)
				{
					if (arr.get(i).equals("values"))
					{
						index = i + 1;
						break;
					}
					else
					{
						sql += arr.get(i) + ", ";
					}
				}
				if (sql.endsWith(", "))
				{
					sql = sql.substring(0, sql.length() - 2);
					sql += ")";
				}
				sql += " VALUES (";
				for (int i = index; i < arr.size(); i++)
				{
					sql += "\"" + arr.get(i) + "\", ";
				}
				if (sql.endsWith(", "))
				{
					sql = sql.substring(0, sql.length() - 2);
					sql += ")";
				}
			}
			sql += ";";
			System.out.println("\nSQL: " + sql + "\n");
			int rs = stmt.executeUpdate(sql);
			answer.add("" + rs);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return answer;
	}

	protected void clientConnected(ConnectionToClient client)
	{
		System.out.println("Client " + client.getId() + " connected, " + getNumberOfClients() + " clients are online");
	}

	public static void main(String[] args) throws IOException
	{
		int port = DEFAULT_PORT;

		SchoolServer sv = new SchoolServer(port);
		try
		{
			sv.listen();
		}
		catch (Exception ex)
		{
			System.out.println("ERROR - Could not listen for clients!");
		}
	}
}
