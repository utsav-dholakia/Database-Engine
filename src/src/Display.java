package src;
import java.io.File;

public class Display {
	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*",80));
		System.out.println();
		System.out.println("\tAll commands are case-insensitive.");
		System.out.println("\tSHOW SCHEMAS;   Display all schemas.");
		System.out.println("\tSHOW TABLES;   Display all tables in the current schema.");
		System.out.println("\tUSE <SCHEMA NAME>;     Use the given schema as the default from now on.");
		System.out.println("\tCREATE SCHEMA <SCHEMA NAME>;   Create a new schema with name : <SCHEMA NAME>");
		System.out.println("\tCREATE TABLE <TABLE NAME>;   Create a new table with name : <TABLE NAME>");
		System.out.println("\tSELECT * FROM <TABLE NAME>;   Display all records from table with name : <TABLE NAME>");
		System.out.println("\tSELECT * FROM <TABLE NAME> WHERE <COLUMN NAME> <'<' OR '>' OR '='> <VALUE>;   Display all records from table with name : <TABLE NAME> where the value matches the value in the given column for that record");
		System.out.println("\tINSERT INTO TABLE <TABLE NAME> VALUES (<COLUMN NAME> <DATE TYPE> [<'PRIMARY KEY' OR 'NOT NULL'>],...);   Display all records from table with name : <TABLE NAME>");
		System.out.println("\tVERSION;       Show the program version.");
		System.out.println("\tHELP;          Show this help information");
		System.out.println("\tEXIT;          Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}
	
	/**
	 *  Display the welcome "splash screen"
	 */
	public static void splashScreen() {
		try
		{
			/* Load information schema tables from binary data file */
			/*File schemataFile = new File("./Schemas/" + information_schemaSchemataTable);
			File tablesFile = new File("./Schemas/" + information_schemaTablesTable);
			File columnsFile = new File("./Schemas/" + information_schemaColumnsTable);*/
			File schemaFiles = new File ("./Schemas");
			
			if(!schemaFiles.exists())
			{
				Schema.createSchema();
			}
			else
			{
				Schema.loadSchema();
			}
			System.out.println(line("*",80));
	        System.out.println("Welcome to UtsavDB"); // Display the string.
			version();
			System.out.println("Type \"help;\" to display supported commands.");
			System.out.println(line("*",80));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	/**
	 * @param num The number of newlines to be displayed to <b>stdout</b>
	 */
	public static void newline(int num) {
		for(int i=0;i<num;i++) {
			System.out.println();
		}
	}
	
	public static void version() {
		System.out.println("UtsavDB v1.0\n");
	}

}
