package pgreport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

/*
 * java -cp pgreport-0.0.1-SNAPSHOT-jar-with-dependencies.jar pgreport.ReportGenerator
 */

public class ReportGenerator {

	public static String tenantId = null;

	public static void main(String args[]) throws Exception {

		// add try
		try {
			if (args[0] != null && args[0] != "") {
				tenantId = args[0];
				ReportGenerator rg = new ReportGenerator();
				rg.run();
			}
		} catch (Exception e) {
			printUsage();
		}
	}

	private static void printUsage() {

		System.out.println("java -cp <jarfile>jar pgreport.ReportGenerator <1234>\r\n" + "where 1234 is tenantid");
	}

	private void run() {

		String url = System.getenv("url_ems");
		String username = System.getenv("username");
		String password = System.getenv("password");
		String person_count_sql = getPersonCountSQL();
		String person_detail_sql = getPersonAndGroupDetailsSQL();
		String aprj_count_sql = getAPRJSONCountSQL();
		String aprj_detail_sql = getAPRJSONDetailsSQL();

		testPersonTable(url, username, password, person_count_sql, person_detail_sql);

		testAPRJTable(username, password, aprj_count_sql, aprj_detail_sql);

	}

	private void testPersonTable(String url, String username, String password, String person_count_sql,
			String person_detail_sql) {
		try (Connection con = DriverManager.getConnection(url, username, password)) {

			Statement stmt = con.createStatement();

			System.out.println("TESTING PERSON TABLE 1");
			try (ResultSet rs = stmt.executeQuery(person_count_sql);) {
				while (rs.next()) {
					System.out.println(rs.getString("rcount"));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

			System.out.println("TESTING PERSON TABLE 2");
			try (ResultSet rs = stmt.executeQuery(person_detail_sql);) {
				while (rs.next()) {
					System.out.println(rs.getString("UPN"));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void testAPRJTable(String username, String password, String aprj_count_sql, String aprj_detail_sql)
			 {
		String url = System.getenv("url_rms");

		try (Connection con = DriverManager.getConnection(url, username, password)) {

			Statement stmt = con.createStatement();

			System.out.println("TESTING AuthorizationPrincipalResourceJSON TABLE 1");
			try (ResultSet rs = stmt.executeQuery(aprj_count_sql);) {
				while (rs.next()) {
					System.out.println(rs.getString("rcount"));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

			System.out.println("TESTING AuthorizationPrincipalResourceJSON TABLE 2");
			try (ResultSet rs = stmt.executeQuery(aprj_detail_sql);) {
				while (rs.next()) {
					System.out.println(rs.getString("body"));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

		} catch(SQLException e) {
			e.printStackTrace();
		}
	}

	private String getPersonCountSQL() {
		return "select count(*) as rcount from view_" + tenantId + ".person";
	}

	private String getAPRJSONCountSQL() {
		return "SELECT count(*) as rcount FROM maas_admin.\"AuthorizationPrincipalResourceJSON_" + tenantId + "\"";
	}

	private String getAPRJSONDetailsSQL() {
		return "SELECT aprjson.id,aprjson.body FROM maas_admin.\"AuthorizationPrincipalResourceJSON_" + tenantId
				+ "\" as aprjson LIMIT 5";
	}

	private String getPersonAndGroupDetailsSQL() {
		return "select p.\"UPN\", p.\"EMAIL\", p.\"FIRSTNAME\", p.\"LASTNAME\", p.\"NAME\", pg.\"NAME\" as \"Group\""
				+ " FROM " + " view_" + tenantId + ".person as p," + " view_" + tenantId + ".persongroup as pg,"
				+ " view_" + tenantId + ".r_persontogroup as rel" + " where p.\"EMAIL\" like '%innovationai.in' and"
				+ " rel.personid = p.\"ID\" and" + " rel.persongroupid = pg.\"ID\" " + " LIMIT 5";
	}
}
