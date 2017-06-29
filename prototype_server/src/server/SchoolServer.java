package server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import ocsf.*;

public class SchoolServer extends AbstractServer {
	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
	private static final File ASSIGNMENTS_DIR = new File("assignments");
	private static final File SOLUTIONS_DIR = new File("solutions");

	final public static int DEFAULT_PORT = 5556;
	ArrayList<String> arr;

	public SchoolServer(int port) {
		super(port);

		// Create the directory if it doesn't exist.
		if (!ASSIGNMENTS_DIR.exists()) {
			ASSIGNMENTS_DIR.mkdir();
		}
		if (!SOLUTIONS_DIR.exists()) {
			SOLUTIONS_DIR.mkdir();
		}
	}

	private void addAssignment(ArrayList<?> msg, ConnectionToClient client) {
		System.out.println("Adding assignment");
		LocalDateTime dueDate = (LocalDateTime) msg.get(1);
		String courseID = (String) msg.get(2);
		byte[] fileContents = (byte[]) msg.get(3);
		String originalFileName = (String) msg.get(4);
		String assignmentName = (String) msg.get(5);

		System.out.println("All arguments OK");
		int dotIndex = originalFileName.lastIndexOf('.');
		String extension = originalFileName.substring(dotIndex);

		String assignmentFileName = assignmentName + extension;

		File output = new File(ASSIGNMENTS_DIR, assignmentFileName);

		try {
			System.out.println("Writing file");
			Files.write(output.toPath(), fileContents);
		} catch (IOException e) {
			System.out.println("Add assignment failed (can't write file)");
			e.printStackTrace();

			return;
		}

		String formattedDate = dueDate.format(FORMATTER);

		System.out.println("FILENAME IS " + assignmentFileName);

		executeInsert("INSERT INTO assignment (assignmentName, dueDate, farmat, courseID) VALUES (?, ?, ?, ?)",
				assignmentFileName, dueDate.format(FORMATTER), extension.substring(1), courseID);

		executeInsert("INSERT INTO assignment_in_course (courseID, assignmentName) VALUES (?, ?)", courseID,
				assignmentFileName);

		System.out.println("OK!");
	}

	private void addSolution(ArrayList<?> msg, ConnectionToClient client) {
		System.out.println("Adding solution");
		String assignmentName = (String) msg.get(1);
		String courseid = (String) msg.get(2);
		String originalFileName = (String) msg.get(3);
		byte[] contents = (byte[]) msg.get(4);

		int dotIndex = originalFileName.lastIndexOf('.');
		String extension = originalFileName.substring(dotIndex);

		String solutionFileName = UUID.randomUUID().toString() + extension;

		File output = new File(SOLUTIONS_DIR, solutionFileName);

		try {
			System.out.println("Writing file");
			Files.write(output.toPath(), contents);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		System.out.println("Wrote to " + output.getAbsolutePath());
		System.out.println("Running INSERT");

		executeInsert(
				"INSERT INTO pupil_assignment (pupilAssignmentName, subbmisionDate, assignmentName) VALUES (?, NOW(), ?)",
				output.getPath(), assignmentName);

		System.out.println("OK!");
	}

	public void handleMessageFromClient(Object msg, ConnectionToClient client) {
		/************************************************ Checks *************************************************/
		System.out.println("Request received from " + client);
		Object response = null;
		if (!(msg instanceof ArrayList<?>) || ((ArrayList<?>) msg).size() < 3) {
			try {
				client.sendToClient(null);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		}

		ArrayList<?> rawMessage = (ArrayList<?>) msg;

		if (rawMessage.get(0).equals("add assignment")) {
			// TODO remove this try-catch
			try {
				addAssignment(rawMessage, client);
			} catch (RuntimeException e) {
				System.out.println("Something went wrong");
				e.printStackTrace(System.out);
			}

			return;
		}
		if (rawMessage.get(0).equals("add assignment solution")) {
			// TODO remove this try-catch
			try {
				addSolution(rawMessage, client);
			} catch (RuntimeException e) {
				System.out.println("Something went wrong");
				e.printStackTrace(System.out);
			}

			return;
		}

		System.out.println("query handler");
		/************************************************
		 * Query handler
		 ******************************************/
		// msg is array list and has at least 2 strings
		arr = (ArrayList<String>) msg;
		String clientId = arr.remove(0);
		String query = arr.remove(0);

		if (query.equals("select")) {
			response = select(arr);
		} else if (query.equals("update")) {
			response = update(arr);
		} else if (query.equals("insert")) {
			response = insert(arr);
		} else if (query.equals("delete")) {
			response = delete(arr);
		} else if (query.equals("select field")) {
			response = selectField(arr);
		} else if (query.equals("histogram 1")) {
			response = histogram1(arr);
		} else if (query.equals("histogram 2")) {
			response = histogram2(arr);
		} else if (query.equals("histogram 3")) {
			response = histogram3(arr);
		}

		System.out.println("sending back response " + response);
		/************************************************
		 * Send to Client
		 ******************************************/
		try {
			if (response != null)
				((ArrayList<String>) response).add(0, clientId);
			System.out.println("sending now");
			client.sendToClient(response);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void serverStarted() {
		System.out.println("Server listening for connections on port " + getPort());
	}

	protected void serverStopped() {
		System.out.println("Server has stopped listening for connections.");
	}

	private void executeInsert(String sql, String... arguments) {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception ex) {
			System.out.println("Error - connection to DB");
			return;
		}

		try {
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			PreparedStatement stmt = conn.prepareStatement(sql);
			for (int i = 0; i < arguments.length; i++) {
				stmt.setString(i + 1, arguments[i]);
			}

			System.out.println("Executing INSERT");
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
			System.out.println("Insert failed! " + e);
		}

	}

	protected Object selectField(ArrayList<String> arr) {
		Statement stmt;
		String sql = "";
		ArrayList<String> answer = new ArrayList<>();
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception ex) {
			System.out.println("Error - connection to DB");
		}
		try {
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0) {
				// error handling
				return null;
			}

			sql = "SELECT  " + arr.get(0) + " FROM " + arr.get(1);

			if (arr.size() > 1) {

				if (arr.size() > 2) {
					sql += " WHERE ";
					for (int i = 2; i < arr.size(); i += 2) {
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

				while (rs.next()) {
					String row = "";
					for (int i = 1; i <= count; i++) {
						row += metaData.getColumnLabel(i) + "=" + rs.getString(i) + ";";
					}
					if (row.endsWith(";"))
						row = row.substring(0, row.length() - 1);
					answer.add(row);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return answer;
	}

	protected Object select(ArrayList<String> arr) {
		Statement stmt;
		String sql = "";
		ArrayList<String> answer = new ArrayList<>();
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception ex) {
			System.out.println("Error - connection to DB");
		}
		try {
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0) {
				// error handling
				return null;
			}

			sql = "SELECT * FROM " + arr.get(0);
			if (arr.size() >= 1) {
				if (arr.size() > 2) {
					sql += " WHERE ";
					for (int i = 1; i < arr.size(); i += 2) {
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

				while (rs.next()) {
					String row = "";
					for (int i = 1; i <= count; i++) {
						row += metaData.getColumnLabel(i) + "=" + rs.getString(i) + ";";
					}
					if (row.endsWith(";"))
						row = row.substring(0, row.length() - 1);
					answer.add(row);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return answer;
	}

	protected Object histogram1(ArrayList<String> arr) {
		Statement stmt;
		String sql = "";
		ArrayList<String> answer = new ArrayList<>();
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception ex) {
			System.out.println("Error - connection to DB");
		}
		try {
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0) {
				// error handling
				return null;
			}

			/*
			 * sql =
			 * "SELECT S.SemesterID, CC.classId, AVG(P.gradeInCourse) AS avgGrade "
			 * +
			 * " FROM course_in_class CC, activity_in_semester S, pupil_in_course P "
			 * + " WHERE CC.teacherId=" + arr.remove(0) + " AND (";
			 */

			// TEST QUERY
			sql = "SELECT CC.classId, AVG(P.gradeInCourse) AS avgGrade "
					+ " FROM course_in_class CC, activity_in_semester S, pupil_in_course P " + " WHERE CC.teacherId="
					+ arr.remove(0) + " AND (";

			for (int i = 0; i < arr.size(); i++)
				sql += "S.SemesterID=" + arr.get(i) + " OR ";

			if (sql.endsWith("OR "))
				sql = sql.substring(0, sql.length() - 3);
			sql += ") AND CC.courseId=S.ActivityID AND CC.courseId=P.courseID " + " GROUP BY CC.classId;";

			System.out.println("\nSQL: " + sql + "\n");
			ResultSet rs = stmt.executeQuery(sql);
			// need to change "is Logged" field!!!

			ResultSetMetaData metaData = rs.getMetaData();
			int count = metaData.getColumnCount(); // number of column

			while (rs.next()) {
				String row = "";
				for (int i = 1; i <= count; i++) {
					row += metaData.getColumnLabel(i) + "=" + rs.getString(i) + ";";
				}
				if (row.endsWith(";"))
					row = row.substring(0, row.length() - 1);
				answer.add(row);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return answer;
	}

	protected Object histogram2(ArrayList<String> arr) {
		Statement stmt;
		String sql = "";
		ArrayList<String> answer = new ArrayList<>();
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception ex) {
			System.out.println("Error - connection to DB");
		}
		try {
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0) {
				// error handling
				return null;
			}

			sql = "SELECT CC.teacherId, AVG(P.gradeInCourse) AS avgGrade "
					+ " FROM course_in_class CC, activity_in_semester S, pupil_in_course P " + " WHERE CC.classId="
					+ arr.remove(0) + " AND (";

			for (int i = 0; i < arr.size(); i++)
				sql += "S.SemesterID=" + arr.get(i) + " OR ";

			if (sql.endsWith("OR "))
				sql = sql.substring(0, sql.length() - 3);

			sql += ") AND CC.courseId=S.ActivityID AND CC.courseId=P.courseID " + " GROUP BY CC.teacherId;";

			System.out.println("\nSQL: " + sql + "\n");
			ResultSet rs = stmt.executeQuery(sql);
			// need to change "is Logged" field!!!

			ResultSetMetaData metaData = rs.getMetaData();
			int count = metaData.getColumnCount(); // number of column

			while (rs.next()) {
				String row = "";
				for (int i = 1; i <= count; i++) {
					row += metaData.getColumnLabel(i) + "=" + rs.getString(i) + ";";
				}
				if (row.endsWith(";"))
					row = row.substring(0, row.length() - 1);
				answer.add(row);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return answer;
	}

	protected Object histogram3(ArrayList<String> arr) {
		Statement stmt;
		String sql = "";
		ArrayList<String> answer = new ArrayList<>();
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception ex) {
			System.out.println("Error - connection to DB");
		}
		try {
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0) {
				// error handling
				return null;
			}

			sql = "SELECT CC.courseId, AVG(P.gradeInCourse) AS avgGrade "
					+ " FROM course_in_class CC, activity_in_semester S, pupil_in_course P " + " WHERE CC.classId="
					+ arr.get(0) + " AND (";

			for (int i = 0; i < arr.size(); i++)
				sql += "S.SemesterID=" + arr.get(i) + " OR ";

			if (sql.endsWith("OR "))
				sql = sql.substring(0, sql.length() - 3);

			sql += ") AND CC.courseId=S.ActivityID AND CC.courseId=P.courseID " + " GROUP BY CC.courseId;";

			// TEST QUERY
			/*
			 * sql = "SELECT CC.classId, AVG(P.gradeInCourse) AS avgGrade " +
			 * " FROM course_in_class CC, activity_in_semester S, pupil_in_course P "
			 * + " WHERE CC.teacherId=" + arr.remove(0) + " AND (";
			 * 
			 * for (int i = 0; i < arr.size(); i++) sql += "S.SemesterID=" +
			 * arr.get(i) + " OR ";
			 * 
			 * if (sql.endsWith("OR ")) sql = sql.substring(0, sql.length() -
			 * 3); sql +=
			 * ") AND CC.courseId=S.ActivityID AND CC.courseId=P.courseID " +
			 * " GROUP BY CC.classId;";
			 * 
			 * System.out.println("\nSQL: " + sql + "\n");
			 */

			System.out.println("\nSQL: " + sql + "\n");
			ResultSet rs = stmt.executeQuery(sql);

			ResultSetMetaData metaData = rs.getMetaData();
			int count = metaData.getColumnCount(); // number of column

			while (rs.next()) {
				String row = "";
				for (int i = 1; i <= count; i++) {
					row += metaData.getColumnLabel(i) + "=" + rs.getString(i) + ";";
				}
				if (row.endsWith(";"))
					row = row.substring(0, row.length() - 1);
				answer.add(row);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return answer;
	}

	protected Object update(ArrayList<String> arr) {
		Statement stmt;
		String sql = "";
		int index = 0;
		ArrayList<String> answer = new ArrayList<>();
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception ex) {
			System.out.println("Error - connection to DB");
		}
		try {
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0) {
				// error handling
				return null;
			}

			sql = "UPDATE " + arr.get(0);
			if (arr.size() > 1) {
				sql += " SET ";
				for (int i = 1; i < arr.size(); i += 2) {
					if (arr.get(i).equals("conditions")) {
						index = i + 1;
						break;
					} else {
						sql += arr.get(i) + "=\"" + arr.get(i + 1) + "\" ";
						if (i + 2 < arr.size())
							sql += ", ";
					}
				}
				if (sql.endsWith(", "))
					sql = sql.substring(0, sql.length() - 2);
				if (index != 0) {
					sql += " WHERE ";
					for (int i = index; i < arr.size(); i += 2) {
						sql += arr.get(i) + "=\"" + arr.get(i + 1) + "\" ";
						if (i + 2 < arr.size())
							sql += "AND ";
					}
				} else {
					System.out.println("Error - No Condition for WHERE");
					return null;
				}

			}
			sql += ";";
			System.out.println("\nSQL: " + sql + "\n");
			int rs = stmt.executeUpdate(sql);
			answer.add("" + rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return answer;
	}

	protected Object delete(ArrayList<String> arr) {
		Statement stmt;
		String sql = "";
		ArrayList<String> answer = new ArrayList<>();
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception ex) {
			System.out.println("Error - connection to DB");
		}
		try {
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0) {
				// error handling
				return null;
			}

			sql = "DELETE FROM " + arr.get(0);
			if (arr.size() >= 3) {
				sql += " WHERE ";
				for (int i = 1; i < arr.size(); i += 2) {
					sql += arr.get(i) + "=\"" + arr.get(i + 1) + "\" ";
					if (i + 2 < arr.size())
						sql += "AND ";
				}
			}
			sql += ";";
			System.out.println("\nSQL: " + sql + "\n");
			int rs = stmt.executeUpdate(sql);
			answer.add("" + rs);

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return answer;
	}

	protected Object insert(ArrayList<String> arr) {
		Statement stmt;
		String sql = "";
		ArrayList<String> answer = new ArrayList<>();
		int index = 0;
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception ex) {
			System.out.println("Error - connection to DB");
		}
		try {
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/school", "root", "admin");
			stmt = conn.createStatement();

			if (arr.size() == 0) {
				// error handling
				return null;
			}

			sql = "INSERT INTO " + arr.get(0);
			if (arr.size() >= 4) {
				sql += " (";
				for (int i = 1; i < arr.size(); i++) {
					if (arr.get(i).equals("values")) {
						index = i + 1;
						break;
					} else {
						sql += arr.get(i) + ", ";
					}
				}
				if (sql.endsWith(", ")) {
					sql = sql.substring(0, sql.length() - 2);
					sql += ")";
				}
				sql += " VALUES (";
				for (int i = index; i < arr.size(); i++) {
					sql += "\"" + arr.get(i) + "\", ";
				}
				if (sql.endsWith(", ")) {
					sql = sql.substring(0, sql.length() - 2);
					sql += ")";
				}
			}
			sql += ";";
			System.out.println("\nSQL: " + sql + "\n");
			int rs = stmt.executeUpdate(sql);
			answer.add("" + rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return answer;
	}

	protected void clientConnected(ConnectionToClient client) {
		System.out.println("Client " + client.getId() + " connected, " + getNumberOfClients() + " clients are online");
	}

	public static void main(String[] args) throws IOException {
		int port = DEFAULT_PORT;

		SchoolServer sv = new SchoolServer(port);
		try {
			sv.listen();
		} catch (Exception ex) {
			System.out.println("ERROR - Could not listen for clients!");
		}
	}
}
