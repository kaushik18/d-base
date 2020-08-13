// Kaushik Nadimpalli
// DavisBase.java - DavisBase Project
// Code adapted/extended  from Stub code -  with additonal methods - provided in class
// These methods parse user command query and call the necessary method in Utilities.java

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DavisBase {
	static String userinput = "davibase-db> ";
	static String version = "V1-SU2020";
	static boolean activeDB = false;
	static long pageSize = 512;
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	public static void main(String[] args) throws FileNotFoundException {
		splashScreen();
		String userCommand = "";
		startDavisBase();
		while (!activeDB) {
			System.out.print(userinput);
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("You have quit DavisBase.");
	}

	public static void splashScreen() {
		System.out.println("------------------------------------------------------------------------------------------------------");
		System.out.println("Welcome to DavisBase");
		System.out.println("DavisBase Version " + getVersion());
		System.out.println("\nType \"help;\" to display helpful commands.");
    System.out.println("------------------------------------------------------------------------------------------------------");
	}

	public static String getVersion() { return version; }
	public static void displayVersion() { System.out.println("DavisBase Version " + getVersion());}
  public static void parseUserCommand(String userCommand) throws FileNotFoundException {
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		switch (commandTokens.get(0)) {
		case "show":
			String[] condition = new String[0];
			String[] columnNames = { "*" };
			Utilities.userQuery("davisbase_tables", columnNames, condition);
			break;
		case "select":
			System.out.println("QUERY CASE: SELECT");
			parseQueryCommand(userCommand);
			break;
		case "drop":
			System.out.println("QUERY CASE: DROP");
			dropTable(userCommand);
			break;
		case "create":
			System.out.println("QUERY CASE: CREATE");
			parseCreateCommand(userCommand);
			break;
		case "update":
			System.out.println("QUERY: UPDATE");
			parseUpdateCommand(userCommand);
			break;
		case "delete":
      System.out.println("QUERY: DELETE");
			parseDeleteCommand(userCommand);
			break;
		case "insert":
      System.out.println("QUERY: INSERT");
			parseInsertCommand(userCommand);
			break;
    case "help":
      help();
      break;
    case "version":
      displayVersion();
      break;
    case "exit":
      activeDB = true;
      break;
		case "quit":
			activeDB = true;
		default:
			System.out.println("Cannot parse:: \"" + userCommand + "\"");
      System.out.println("Please use an acceptable command.");
      System.out.println();
			break;
		}
	}

	public static boolean findTable(String tableName) {
		String filename = tableName + ".tbl";
		File catalog = new File("data/catalog/");
		String[] tablenames = catalog.list();
		for (String table : tablenames) {
			if (filename.equals(table))
				return true;
		}
		File userdata = new File("data/userdata/");
		String[] tables = userdata.list();
		for (String table : tables) {
			if (filename.equals(table))
				return true;
		} return false;
	}
	private static void parseDeleteCommand(String userCommand) {
		String[] delete = userCommand.split("where");
		String[] table = delete[0].trim().split("from");
		String[] table1 = table[1].trim().split(" ");
		String tableName = table1[1].trim();
		String[] cond = Utilities.queryParse(delete[1]);
		if (!findTable(tableName)) {
			System.out.println("This table does not exist.");
			return;
		}
		try {
			Utilities.deleteTable(tableName, cond);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void dropTable(String dropTableString) throws FileNotFoundException {
		String[] droptemp = dropTableString.split(" ");

		String tableName = droptemp[2].trim();

		if (!findTable(tableName)) {
			System.out.println("Table " + tableName + " does not exist.");
			System.out.println();
		} else {
			Utilities.dropTable(tableName);
		}
	}

  public static void parseInsertCommand(String insertString) {
		String[] insert = insertString.split(" ");
		String tableName = insert[2].trim();
		String values = insertString.split("values")[1].replaceAll("\\(", "").replaceAll("\\)", "").trim();
		String[] insertValues = values.split(",");
		for (int i = 0; i < insertValues.length; i++)
			insertValues[i] = insertValues[i].trim();
		if (!findTable(tableName)) {
			System.out.println("Table " + tableName + " does not exist.");
			System.out.println();
			return;
		} else
			Utilities.insertIntoTable(tableName, insertValues);
	}

	private static void parseUpdateCommand(String userCommand) {
		String[] updates = userCommand.toLowerCase().split("set");
		String[] table = updates[0].trim().split(" ");
		String tablename = table[1].trim();
		String set_value;
		String where = null;
		if (!findTable(tablename)) {
			System.out.println("This table does not exist.");
			return;
		}
		if (updates[1].contains("where")) {
			String[] findupdate = updates[1].split("where");
			set_value = findupdate[0].trim();
			where = findupdate[1].trim();
			Utilities.updateTable(tablename, Utilities.queryParse(set_value), Utilities.queryParse((where)));
		} else {
			set_value = updates[1].trim();
			String[] no_where = new String[0];
			Utilities.updateTable(tablename, Utilities.queryParse(set_value), no_where);
		}
	}

	public static void parseCreateCommand(String createTableString) {
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));
		String tableName = createTableTokens.get(2);
		String[] temp = createTableString.replaceAll("\\(", "").replaceAll("\\)", "").split(tableName);
		String[] columnNames = temp[1].trim().split(",");
		for (int i = 0; i < columnNames.length; i++)
			columnNames[i] = columnNames[i].trim();
		if (findTable(tableName)) {
			System.out.println("Table " + tableName + " exists.");
			System.out.println();
		} else {
			RandomAccessFile table;
			try {
				table = new RandomAccessFile("data/userdata/" + tableName + ".tbl", "rw");
				Utilities.createTable(table, tableName, columnNames);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public static void parseQueryCommand(String userQueryString) {
		String tableName;
		String[] columnNames;
		String[] condition = new String[0];
		String temp[] = userQueryString.split("where");
		tableName = temp[0].split("from")[1].trim();
		columnNames = temp[0].split("from")[0].replaceAll("select", " ").split(",");
		if (!findTable(tableName)) {
			System.out.println("This table does not exist.");
		}
		else {
			for (int i = 0; i < columnNames.length; i++)
				columnNames[i] = columnNames[i].trim();
			if (temp.length > 1)
				condition = Utilities.queryParse(temp[1]);
			Utilities.userQuery(tableName, columnNames, condition);
		}
	}

  public static void help() {
    System.out.println("------------------------------------------------------------------------------------------------------");
    System.out.println("SUPPORTED COMMANDS\n");
    System.out.println("All commands below are case insensitive\n");
    System.out.println("SHOW TABLES;");
    System.out.println("\tDisplay the names of all tables.\n");
    System.out.println("SELECT âŸ¨column_listâŸ© FROM table_name [WHERE condition];\n");
    System.out.println("\tDisplay table records whose optional condition");
    System.out.println("\tis <column_name> = <value>.\n");
    System.out.println("INSERT INTO (column1, column2, ...) table_name VALUES (value1, value2, ...);\n");
    System.out.println("\tInsert new record into the table.");
    System.out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
    System.out.println("\tModify records data whose optional <condition> is\n");
    System.out.println("DELETE TABLE table_name where <column_name> <operator> <value>;");
    System.out.println("\tDeletes selected record from table.\n");
    System.out.println("DROP TABLE table_name;");
    System.out.println("\tRemove table data (i.e. all records) and its schema.\n");
    System.out.println("VERSION;");
    System.out.println("\tDisplay the program version.\n");
    System.out.println("HELP;");
    System.out.println("\tDisplay this help information.\n");
    System.out.println("EXIT;");
    System.out.println("\tExit the program.\n");
    System.out.println("------------------------------------------------------------------------------------------------------");
  }

	public static void startDavisBase() {
		File file = new File("data/catalog");
		File userData = new File("data/userdata");
		file.mkdirs();
		userData.mkdirs();
		if (file.isDirectory()) {
			File davisBaseTables = new File("data/catalog/davisbase_tables.tbl");
			File davisBaseColumns = new File("data/catalog/davisbase_columns.tbl");
			if (!davisBaseTables.exists()) {
				Utilities.defineDB();
			}
			if (!davisBaseColumns.exists()) {
				Utilities.defineDB();
			}
		} else {
			Utilities.defineDB();
		}
	}
}
