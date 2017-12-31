package src;
import java.io.RandomAccessFile;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Utsav Vijay Dholakia
 * @version 1.0
 * <b>This is an implementation of UtsavDB</b>
 *
 */
public class UtsavDB {
	
	public static void main(String[] args) {
		/* Display the welcome splash screen */
		Display.splashScreen();
		
		Scanner scanner = new Scanner(System.in).useDelimiter(";");
		String userCommand; // Variable to collect user input from the prompt
		boolean flag = false;	//Checked if the exit command entered or not and its response status
		
		do {  // do-while !exit
			System.out.print(Properties.prompt);
			userCommand = scanner.next().trim();
			String [] command = new String[25];
			command = userCommand.split(" ", 3);

			if(command[0].equals("USE")||command[0].equals("use"))
			{
				Commands.changeSchema(command[1]);
			}
			else
			{
				switch (command[0]) 
				{
					case "show":
					case "SHOW":
						if(command[1].equals("SCHEMAS")||command[1].equals("schemas"))
						{
							Commands.showAllSchemas();
						}
						else if(command[1].equals("TABLES")||command[1].equals("tables"))
						{
							Commands.showAllTables();
						}
						break;
					
					case "create":
					case "CREATE":
						if(command[1].equals("SCHEMA")||command[1].equals("schema"))
						{
							Commands.createSchema(command[2]);
						}
						else if((command[1].equals("TABLE")||command[1].equals("table")) && command.length > 2)
						{
							String[] query = command[2].toString().split(" ",2);
							String tableName = query[0];
							System.out.println("table name: "+tableName);
							String regex = "(\\w+)\\s(CHAR|FLOAT|INT|SHORT INT|SHORT|LONG INT|LONG|DATETIME|DATE|DOUBLE|VARCHAR|BYTE|"
									+ "char|float|int|short int|short|long int|long|datetime|date|double|varchar|byte)"
									+ "(\\((\\d+)\\))?((\\sPRIMARY KEY|\\sNOT NULL|\\sprimary key|\\snot null)?)";
							Pattern pttern = Pattern.compile(regex);
							Matcher m = pttern.matcher(query[1]);
							Commands.createTable(tableName,m);
						}
						else
						{
							System.out.println("Check your query syntax");
						}
							
						break;
					
					case "select":
					case "SELECT":
						if(command[1].equals("*"))
						{
							String[] query = command[2].toString().split(" ",6);
							if(query.length <= 2)
							{
								String tableName = query[1];
								Commands.select(tableName);
							}
							else if(query.length == 6)
							{
								String tableName = query[1];
								String columnName = query[3];
								String operator = query[4];
								String regex = "(\\w+[\\s\\w]*|-|:|\\.|_)+";
								Pattern pttern = Pattern.compile(regex);
								Matcher m = pttern.matcher(query[5]);
								String value = "";
								while(m.find())
								{
									value += m.group();
								}
								Commands.selectWhere(tableName, columnName, operator, value);
							}
							else
							{
								System.out.println("Give proper spaces in your query, check help screen for syntax");
							}
						}
						else
						{
							System.out.println("You can't provide column names in select clause, check your query");
						}
						break;
						
					case "insert":
					case "INSERT":
						if(command[1].equals("INTO")||command[1].equals("into"))
						{
							String [] query = command[2].toString().split(" ",4);
							if(query.length < 4)
							{
								System.out.println("Query syntax error (length < 4), check help");
								break;
							}
							if(!query[0].equals("TABLE") && !query[0].equals("table"))
							{
								System.out.println("Query syntax error (table keyword not correct), check help");
								break;
							}
							if(!query[2].equals("VALUES") && !query[2].equals("values"))
							{
								System.out.println("Query syntax error (values keyword not correct), check help");
								break;
							}
							String tableName = query[1];
							//System.out.println(tableName);
							//System.out.println(query[3]);
							String regex = "(\\w+[\\s\\w]*|-|:|\\.)+";
							Pattern pttern = Pattern.compile(regex);
							Matcher m = pttern.matcher(query[3]);
							String[] values = new String[50];
							int  i = 0;
							while(m.find( )) 
							{
						         values[i] = m.group();
						         i++;
						    }
							Commands.insert(tableName, values);
						}
						else
						{
							System.out.println("Query syntax error (into keyword not correct), check help");
						}
						break;
						
					case "help":
					case "HELP":
						Display.help();
						break;
						
					case "version":
					case "VERSION":
						Display.version();
						break;
						
					case "exit":
					case "EXIT" :
						if(userCommand.equals("EXIT")||userCommand.equals("exit"))
						{
							flag = saveAndExit();
							if(flag)
								System.out.println("Exiting...");
							else
								System.out.println("Couldn't write data to disc properly, please try again...");
						}
						break;
						
					default:
						System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				}
			}
			
		} while(!flag);
		
	    
    } /* End main() method */


//  ===========================================================================
//  STATIC METHOD DEFINTIONS BEGIN HERE
//  ===========================================================================

	
	public static boolean saveAndExit()
	{
		try
		{
			RandomAccessFile infoschemaSchemataSchemaNameIndex = new RandomAccessFile("./Schemas/information_schema/Index/" + "information_schema.schemata.schema_name.ndx", "rw");
			RandomAccessFile infoschemaTablesTableSchemaIndex = new RandomAccessFile("./Schemas/information_schema/Index/" + "information_schema.tables.table_schema.ndx", "rw");
			RandomAccessFile infoschemaTablesTableNameIndex = new RandomAccessFile("./Schemas/information_schema/Index/" + "information_schema.tables.table_name.ndx", "rw");
			RandomAccessFile infoschemaTablesTableRowsIndex = new RandomAccessFile("./Schemas/information_schema/Index/" + "information_schema.tables.table_rows.ndx", "rw");
			RandomAccessFile infoschemaColumnsTableSchemaIndex = new RandomAccessFile("./Schemas/information_schema/Index/" + "information_schema.columns.table_schema.ndx", "rw");
			RandomAccessFile infoschemaColumnsTableNameIndex = new RandomAccessFile("./Schemas/information_schema/Index/" + "information_schema.columns.table_name.ndx", "rw");
			RandomAccessFile infoschemaColumnsColumnNameIndex = new RandomAccessFile("./Schemas/information_schema/Index/" + "information_schema.columns.column_name.ndx", "rw");
			RandomAccessFile infoschemaColumnsOrdinalPositionIndex = new RandomAccessFile("./Schemas/information_schema/Index/" + "information_schema.columns.ordinal_position.ndx", "rw");
			RandomAccessFile infoschemaColumnsColumnTypeIndex = new RandomAccessFile("./Schemas/information_schema/Index/" + "information_schema.columns.column_type.ndx", "rw");
			RandomAccessFile infoschemaColumnsIsNullableIndex = new RandomAccessFile("./Schemas/information_schema/Index/" + "information_schema.columns.is_nullable.ndx", "rw");
			RandomAccessFile infoschemaColumnsColumnKeyIndex = new RandomAccessFile("./Schemas/information_schema/Index/" + "information_schema.columns.column_key.ndx", "rw");
			//Write Schemata Table index to file from Map
			Schema.writeVarcharIndexToFile(Schema.infoschemaSchemataSchemaNameMap, infoschemaSchemataSchemaNameIndex, 0);
			//Write Tables Table indexes to file from Maps
			Schema.writeVarcharIndexToFile(Schema.infoschemaTablesTableSchemaMap, infoschemaTablesTableSchemaIndex, 0);
			Schema.writeVarcharIndexToFile(Schema.infoschemaTablesTableNameMap, infoschemaTablesTableNameIndex, 0);
			Schema.writeLongIndexToFile(Schema.infoschemaTablesTableRowsMap, infoschemaTablesTableRowsIndex, 0);
			//Schema.write Columns table indexes to file from Maps
			Schema.writeVarcharIndexToFile(Schema.infoschemaColumnsTableSchemaMap, infoschemaColumnsTableSchemaIndex, 0);
			Schema.writeVarcharIndexToFile(Schema.infoschemaColumnsTableNameMap, infoschemaColumnsTableNameIndex, 0);
			Schema.writeVarcharIndexToFile(Schema.infoschemaColumnsColumnNameMap, infoschemaColumnsColumnNameIndex, 0);
			Schema.writeIntIndexToFile(Schema.infoschemaColumnsOrdinalPositionMap, infoschemaColumnsOrdinalPositionIndex, 0);
			Schema.writeVarcharIndexToFile(Schema.infoschemaColumnsColumnTypeMap, infoschemaColumnsColumnTypeIndex, 0);
			Schema.writeVarcharIndexToFile(Schema.infoschemaColumnsIsNullableMap, infoschemaColumnsIsNullableIndex, 0);
			Schema.writeVarcharIndexToFile(Schema.infoschemaColumnsColumnKeyMap, infoschemaColumnsColumnKeyIndex, 0);
	
			return true; //Files written successfully
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return false;	//Files couldn't be written
		}
	}

}



