package src;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Schema{
	/** 
	 * Create Schema for the first time
	 * 
	 */
	static String selectedSchema = "information_schema"; //Global variable that stores which schema is selected
	static TreeMap <String,VarcharIndexClass> infoschemaSchemataSchemaNameMap = new TreeMap <String,VarcharIndexClass> ();
	static TreeMap <String,VarcharIndexClass> infoschemaTablesTableSchemaMap = new TreeMap <String,VarcharIndexClass> ();
	static TreeMap <String,VarcharIndexClass> infoschemaTablesTableNameMap = new TreeMap <String,VarcharIndexClass> ();
	static TreeMap <Long,LongIndexClass> infoschemaTablesTableRowsMap = new TreeMap <Long,LongIndexClass> ();
	static TreeMap <String,VarcharIndexClass> infoschemaColumnsTableSchemaMap = new TreeMap <String,VarcharIndexClass> ();
	static TreeMap <String,VarcharIndexClass> infoschemaColumnsTableNameMap = new TreeMap <String,VarcharIndexClass> ();
	static TreeMap <String,VarcharIndexClass> infoschemaColumnsColumnNameMap = new TreeMap <String,VarcharIndexClass> ();
	static TreeMap <Integer,IntIndexClass> infoschemaColumnsOrdinalPositionMap = new TreeMap <Integer,IntIndexClass> ();
	static TreeMap <String,VarcharIndexClass> infoschemaColumnsColumnTypeMap = new TreeMap <String,VarcharIndexClass> ();
	static TreeMap <String,VarcharIndexClass> infoschemaColumnsIsNullableMap = new TreeMap <String,VarcharIndexClass> ();
	static TreeMap <String,VarcharIndexClass> infoschemaColumnsColumnKeyMap = new TreeMap <String,VarcharIndexClass> ();
	
	public static void createSchema()
	{
		//System.out.println("Creating information schema");
		try
		{
			File file = new File("./Schemas/information_schema/Data/");
			file.mkdirs();
			file = new File("./Schemas/information_schema/Index/");
			file.mkdirs();
			
			RandomAccessFile schemataTableFile = new RandomAccessFile("./Schemas/information_schema/Data/" + Properties.information_schemaSchemataTable, "rw");
			RandomAccessFile tablesTableFile = new RandomAccessFile("./Schemas/information_schema/Data/" + Properties.information_schemaTablesTable, "rw");
			RandomAccessFile columnsTableFile = new RandomAccessFile("./Schemas/information_schema/Data/" + Properties.information_schemaColumnsTable, "rw");
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
			
			/*
			 *  Create the SCHEMATA table file.
			 *  Initially it has only one entry:
			 *      information_schema
			 */
			// ROW 1: information_schema.schemata.tbl
			writeVarcharIndexToMap("information_schema", schemataTableFile.getFilePointer(), infoschemaSchemataSchemaNameMap);
			schemataTableFile.writeByte("information_schema".length());
			schemataTableFile.writeBytes("information_schema");
			
			/*
			 *  Create the TABLES table file.
			 *  Remember!!! Column names are not stored in the tables themselves
			 *              The column names (TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS)
			 *              and their order (ORDINAL_POSITION) are encoded in the
			 *              COLUMNS table.
			 *  Initially it has three rows (each row may have a different length):
			 */
			// ROW 1: information_schema.tables.tbl
			writeVarcharIndexToMap("information_schema", tablesTableFile.getFilePointer(), infoschemaTablesTableSchemaMap);
			writeVarcharIndexToMap("SCHEMATA", tablesTableFile.getFilePointer(), infoschemaTablesTableNameMap);
			writeLongIndexToMap((long)1, tablesTableFile.getFilePointer(), infoschemaTablesTableRowsMap);
			tablesTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			tablesTableFile.writeBytes("information_schema");
			tablesTableFile.writeByte("SCHEMATA".length()); // TABLE_NAME
			tablesTableFile.writeBytes("SCHEMATA");
			tablesTableFile.writeLong(1); // TABLE_ROWS

			// ROW 2: information_schema.tables.tbl
			writeVarcharIndexToMap("information_schema", tablesTableFile.getFilePointer(), infoschemaTablesTableSchemaMap);
			writeVarcharIndexToMap("TABLES", tablesTableFile.getFilePointer(), infoschemaTablesTableNameMap);
			writeLongIndexToMap((long)3, tablesTableFile.getFilePointer(), infoschemaTablesTableRowsMap);
			tablesTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			tablesTableFile.writeBytes("information_schema");
			tablesTableFile.writeByte("TABLES".length()); // TABLE_NAME
			tablesTableFile.writeBytes("TABLES");
			tablesTableFile.writeLong(3); // TABLE_ROWS

			// ROW 3: information_schema.tables.tbl
			writeVarcharIndexToMap("information_schema", tablesTableFile.getFilePointer(), infoschemaTablesTableSchemaMap);
			writeVarcharIndexToMap("COLUMNS", tablesTableFile.getFilePointer(), infoschemaTablesTableNameMap);
			writeLongIndexToMap((long)11, tablesTableFile.getFilePointer(), infoschemaTablesTableRowsMap);
			tablesTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			tablesTableFile.writeBytes("information_schema");
			tablesTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			tablesTableFile.writeBytes("COLUMNS");
			tablesTableFile.writeLong(11); // TABLE_ROWS

			/*
			 *  Create the COLUMNS table file.
			 *  Initially it has 11 rows:
			 */
			// ROW 1: information_schema.columns.tbl
			writeVarcharIndexToMap("information_schema", columnsTableFile.getFilePointer(), infoschemaColumnsTableSchemaMap);
			writeVarcharIndexToMap("SCHEMATA", columnsTableFile.getFilePointer(), infoschemaColumnsTableNameMap);
			writeVarcharIndexToMap("SCHEMA_NAME", columnsTableFile.getFilePointer(), infoschemaColumnsColumnNameMap);
			writeIntIndexToMap(1, columnsTableFile.getFilePointer(), infoschemaColumnsOrdinalPositionMap);
			writeVarcharIndexToMap("varchar(64)", columnsTableFile.getFilePointer(), infoschemaColumnsColumnTypeMap);
			writeVarcharIndexToMap("NO", columnsTableFile.getFilePointer(), infoschemaColumnsIsNullableMap);
			writeVarcharIndexToMap("", columnsTableFile.getFilePointer(), infoschemaColumnsColumnKeyMap);
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("SCHEMATA".length()); // TABLE_NAME
			columnsTableFile.writeBytes("SCHEMATA");
			columnsTableFile.writeByte("SCHEMA_NAME".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("SCHEMA_NAME");
			columnsTableFile.writeInt(1); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 2: information_schema.columns.tbl
			writeVarcharIndexToMap("information_schema", columnsTableFile.getFilePointer(), infoschemaColumnsTableSchemaMap);
			writeVarcharIndexToMap("TABLES", columnsTableFile.getFilePointer(), infoschemaColumnsTableNameMap);
			writeVarcharIndexToMap("TABLE_SCHEMA", columnsTableFile.getFilePointer(), infoschemaColumnsColumnNameMap);
			writeIntIndexToMap(1, columnsTableFile.getFilePointer(), infoschemaColumnsOrdinalPositionMap);
			writeVarcharIndexToMap("varchar(64)", columnsTableFile.getFilePointer(), infoschemaColumnsColumnTypeMap);
			writeVarcharIndexToMap("NO", columnsTableFile.getFilePointer(), infoschemaColumnsIsNullableMap);
			writeVarcharIndexToMap("", columnsTableFile.getFilePointer(), infoschemaColumnsColumnKeyMap);
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("TABLES".length()); // TABLE_NAME
			columnsTableFile.writeBytes("TABLES");
			columnsTableFile.writeByte("TABLE_SCHEMA".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("TABLE_SCHEMA");
			columnsTableFile.writeInt(1); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 3: information_schema.columns.tbl
			writeVarcharIndexToMap("information_schema", columnsTableFile.getFilePointer(), infoschemaColumnsTableSchemaMap);
			writeVarcharIndexToMap("TABLES", columnsTableFile.getFilePointer(), infoschemaColumnsTableNameMap);
			writeVarcharIndexToMap("TABLE_NAME", columnsTableFile.getFilePointer(), infoschemaColumnsColumnNameMap);
			writeIntIndexToMap(2, columnsTableFile.getFilePointer(), infoschemaColumnsOrdinalPositionMap);
			writeVarcharIndexToMap("varchar(64)", columnsTableFile.getFilePointer(), infoschemaColumnsColumnTypeMap);
			writeVarcharIndexToMap("NO", columnsTableFile.getFilePointer(), infoschemaColumnsIsNullableMap);
			writeVarcharIndexToMap("", columnsTableFile.getFilePointer(), infoschemaColumnsColumnKeyMap);
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("TABLES".length()); // TABLE_NAME
			columnsTableFile.writeBytes("TABLES");
			columnsTableFile.writeByte("TABLE_NAME".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("TABLE_NAME");
			columnsTableFile.writeInt(2); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 4: information_schema.columns.tbl
			writeVarcharIndexToMap("information_schema", columnsTableFile.getFilePointer(), infoschemaColumnsTableSchemaMap);
			writeVarcharIndexToMap("TABLES", columnsTableFile.getFilePointer(), infoschemaColumnsTableNameMap);
			writeVarcharIndexToMap("TABLE_ROWS", columnsTableFile.getFilePointer(), infoschemaColumnsColumnNameMap);
			writeIntIndexToMap(3, columnsTableFile.getFilePointer(), infoschemaColumnsOrdinalPositionMap);
			writeVarcharIndexToMap("long int", columnsTableFile.getFilePointer(), infoschemaColumnsColumnTypeMap);
			writeVarcharIndexToMap("NO", columnsTableFile.getFilePointer(), infoschemaColumnsIsNullableMap);
			writeVarcharIndexToMap("", columnsTableFile.getFilePointer(), infoschemaColumnsColumnKeyMap);
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("TABLES".length()); // TABLE_NAME
			columnsTableFile.writeBytes("TABLES");
			columnsTableFile.writeByte("TABLE_ROWS".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("TABLE_ROWS");			
			columnsTableFile.writeInt(3); // ORDINAL_POSITION			
			columnsTableFile.writeByte("long".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("long");			
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 5: information_schema.columns.tbl
			writeVarcharIndexToMap("information_schema", columnsTableFile.getFilePointer(), infoschemaColumnsTableSchemaMap);
			writeVarcharIndexToMap("COLUMNS", columnsTableFile.getFilePointer(), infoschemaColumnsTableNameMap);
			writeVarcharIndexToMap("TABLE_SCHEMA", columnsTableFile.getFilePointer(), infoschemaColumnsColumnNameMap);
			writeIntIndexToMap(1, columnsTableFile.getFilePointer(), infoschemaColumnsOrdinalPositionMap);
			writeVarcharIndexToMap("varchar(64)", columnsTableFile.getFilePointer(), infoschemaColumnsColumnTypeMap);
			writeVarcharIndexToMap("NO", columnsTableFile.getFilePointer(), infoschemaColumnsIsNullableMap);
			writeVarcharIndexToMap("", columnsTableFile.getFilePointer(), infoschemaColumnsColumnKeyMap);
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");
			columnsTableFile.writeByte("TABLE_SCHEMA".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("TABLE_SCHEMA");
			columnsTableFile.writeInt(1); // ORDINAL_POSITION
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 6: information_schema.columns.tbl
			writeVarcharIndexToMap("information_schema", columnsTableFile.getFilePointer(), infoschemaColumnsTableSchemaMap);
			writeVarcharIndexToMap("COLUMNS", columnsTableFile.getFilePointer(), infoschemaColumnsTableNameMap);
			writeVarcharIndexToMap("TABLE_NAME", columnsTableFile.getFilePointer(), infoschemaColumnsColumnNameMap);
			writeIntIndexToMap(2, columnsTableFile.getFilePointer(), infoschemaColumnsOrdinalPositionMap);
			writeVarcharIndexToMap("varchar(64)", columnsTableFile.getFilePointer(), infoschemaColumnsColumnTypeMap);
			writeVarcharIndexToMap("NO", columnsTableFile.getFilePointer(), infoschemaColumnsIsNullableMap);
			writeVarcharIndexToMap("", columnsTableFile.getFilePointer(), infoschemaColumnsColumnKeyMap);
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");			
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");			
			columnsTableFile.writeByte("TABLE_NAME".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("TABLE_NAME");			
			columnsTableFile.writeInt(2); // ORDINAL_POSITION			
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");			
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 7: information_schema.columns.tbl
			writeVarcharIndexToMap("information_schema", columnsTableFile.getFilePointer(), infoschemaColumnsTableSchemaMap);
			writeVarcharIndexToMap("COLUMNS", columnsTableFile.getFilePointer(), infoschemaColumnsTableNameMap);
			writeVarcharIndexToMap("COLUMN_NAME", columnsTableFile.getFilePointer(), infoschemaColumnsColumnNameMap);
			writeIntIndexToMap(3, columnsTableFile.getFilePointer(), infoschemaColumnsOrdinalPositionMap);
			writeVarcharIndexToMap("varchar(64)", columnsTableFile.getFilePointer(), infoschemaColumnsColumnTypeMap);
			writeVarcharIndexToMap("NO", columnsTableFile.getFilePointer(), infoschemaColumnsIsNullableMap);
			writeVarcharIndexToMap("", columnsTableFile.getFilePointer(), infoschemaColumnsColumnKeyMap);
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");			
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");			
			columnsTableFile.writeByte("COLUMN_NAME".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("COLUMN_NAME");			
			columnsTableFile.writeInt(3); // ORDINAL_POSITION			
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");			
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");			
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 8: information_schema.columns.tbl
			writeVarcharIndexToMap("information_schema", columnsTableFile.getFilePointer(), infoschemaColumnsTableSchemaMap);
			writeVarcharIndexToMap("COLUMNS", columnsTableFile.getFilePointer(), infoschemaColumnsTableNameMap);
			writeVarcharIndexToMap("ORDINAL_POSITION", columnsTableFile.getFilePointer(), infoschemaColumnsColumnNameMap);
			writeIntIndexToMap(4, columnsTableFile.getFilePointer(), infoschemaColumnsOrdinalPositionMap);
			writeVarcharIndexToMap("int", columnsTableFile.getFilePointer(), infoschemaColumnsColumnTypeMap);
			writeVarcharIndexToMap("NO", columnsTableFile.getFilePointer(), infoschemaColumnsIsNullableMap);
			writeVarcharIndexToMap("", columnsTableFile.getFilePointer(), infoschemaColumnsColumnKeyMap);
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");			
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");			
			columnsTableFile.writeByte("ORDINAL_POSITION".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("ORDINAL_POSITION");			
			columnsTableFile.writeInt(4); // ORDINAL_POSITION			
			columnsTableFile.writeByte("int".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("int");
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");			
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 9: information_schema.columns.tbl
			writeVarcharIndexToMap("information_schema", columnsTableFile.getFilePointer(), infoschemaColumnsTableSchemaMap);
			writeVarcharIndexToMap("COLUMNS", columnsTableFile.getFilePointer(), infoschemaColumnsTableNameMap);
			writeVarcharIndexToMap("COLUMN_TYPE", columnsTableFile.getFilePointer(), infoschemaColumnsColumnNameMap);
			writeIntIndexToMap(5, columnsTableFile.getFilePointer(), infoschemaColumnsOrdinalPositionMap);
			writeVarcharIndexToMap("varchar(64)", columnsTableFile.getFilePointer(), infoschemaColumnsColumnTypeMap);
			writeVarcharIndexToMap("NO", columnsTableFile.getFilePointer(), infoschemaColumnsIsNullableMap);
			writeVarcharIndexToMap("", columnsTableFile.getFilePointer(), infoschemaColumnsColumnKeyMap);
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");			
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");			
			columnsTableFile.writeByte("COLUMN_TYPE".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("COLUMN_TYPE");			
			columnsTableFile.writeInt(5); // ORDINAL_POSITION			
			columnsTableFile.writeByte("varchar(64)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(64)");			
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");			
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 10: information_schema.columns.tbl
			writeVarcharIndexToMap("information_schema", columnsTableFile.getFilePointer(), infoschemaColumnsTableSchemaMap);
			writeVarcharIndexToMap("COLUMNS", columnsTableFile.getFilePointer(), infoschemaColumnsTableNameMap);
			writeVarcharIndexToMap("IS_NULLABLE", columnsTableFile.getFilePointer(), infoschemaColumnsColumnNameMap);
			writeIntIndexToMap(6, columnsTableFile.getFilePointer(), infoschemaColumnsOrdinalPositionMap);
			writeVarcharIndexToMap("varchar(3)", columnsTableFile.getFilePointer(), infoschemaColumnsColumnTypeMap);
			writeVarcharIndexToMap("NO", columnsTableFile.getFilePointer(), infoschemaColumnsIsNullableMap);
			writeVarcharIndexToMap("", columnsTableFile.getFilePointer(), infoschemaColumnsColumnKeyMap);
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");			
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");			
			columnsTableFile.writeByte("IS_NULLABLE".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("IS_NULLABLE");			
			columnsTableFile.writeInt(6); // ORDINAL_POSITION			
			columnsTableFile.writeByte("varchar(3)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(3)");			
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");			
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");

			// ROW 11: information_schema.columns.tbl
			writeVarcharIndexToMap("information_schema", columnsTableFile.getFilePointer(), infoschemaColumnsTableSchemaMap);
			writeVarcharIndexToMap("COLUMNS", columnsTableFile.getFilePointer(), infoschemaColumnsTableNameMap);
			writeVarcharIndexToMap("COLUMN_KEY", columnsTableFile.getFilePointer(), infoschemaColumnsColumnNameMap);
			writeIntIndexToMap(7, columnsTableFile.getFilePointer(), infoschemaColumnsOrdinalPositionMap);
			writeVarcharIndexToMap("varchar(3)", columnsTableFile.getFilePointer(), infoschemaColumnsColumnTypeMap);
			writeVarcharIndexToMap("NO", columnsTableFile.getFilePointer(), infoschemaColumnsIsNullableMap);
			writeVarcharIndexToMap("", columnsTableFile.getFilePointer(), infoschemaColumnsColumnKeyMap);
			columnsTableFile.writeByte("information_schema".length()); // TABLE_SCHEMA
			columnsTableFile.writeBytes("information_schema");			
			columnsTableFile.writeByte("COLUMNS".length()); // TABLE_NAME
			columnsTableFile.writeBytes("COLUMNS");			
			columnsTableFile.writeByte("COLUMN_KEY".length()); // COLUMN_NAME
			columnsTableFile.writeBytes("COLUMN_KEY");			
			columnsTableFile.writeInt(7); // ORDINAL_POSITION			
			columnsTableFile.writeByte("varchar(3)".length()); // COLUMN_TYPE
			columnsTableFile.writeBytes("varchar(3)");			
			columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
			columnsTableFile.writeBytes("NO");			
			columnsTableFile.writeByte("".length()); // COLUMN_KEY
			columnsTableFile.writeBytes("");
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void loadSchema()
	{
		//System.out.println("Schema already exists");
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
			
			int repeatCount = 0;
			String data = "";
			int dataLength = 0;
			long address = 0;
			VarcharIndexClass varCharValue = new VarcharIndexClass();
			LongIndexClass longValue = new LongIndexClass();
			IntIndexClass intValue = new IntIndexClass();
			
			//Read Schemata Table Schema Name Index file into Map
			while(infoschemaSchemataSchemaNameIndex.getFilePointer() < infoschemaSchemataSchemaNameIndex.length())
			{
				data = "";
				varCharValue = new VarcharIndexClass();
				dataLength = infoschemaSchemataSchemaNameIndex.readByte();
				for(int i = 0; i < dataLength; i++)
				{
					data = data + (char)infoschemaSchemataSchemaNameIndex.readByte();
				}
				varCharValue.data = data;
				repeatCount = infoschemaSchemataSchemaNameIndex.readInt();
				varCharValue.repeatCount = repeatCount;
				for(int i = 0; i < repeatCount; i++)
				{
					address = infoschemaSchemataSchemaNameIndex.readLong();
					varCharValue.address.add(address);
				}
				infoschemaSchemataSchemaNameMap.put(varCharValue.data, varCharValue);
			}
			
			//Read Tables Table Schema Index file into Map
			while(infoschemaTablesTableSchemaIndex.getFilePointer() < infoschemaTablesTableSchemaIndex.length())
			{
				data = "";
				varCharValue = new VarcharIndexClass();
				dataLength = infoschemaTablesTableSchemaIndex.readByte();
				for(int i = 0; i < dataLength; i++)
				{
					data = data + (char)infoschemaTablesTableSchemaIndex.readByte();
				}
				varCharValue.data = data;
				repeatCount = infoschemaTablesTableSchemaIndex.readInt();
				varCharValue.repeatCount = repeatCount;
				for(int i = 0; i < repeatCount; i++)
				{
					address = infoschemaTablesTableSchemaIndex.readLong();
					varCharValue.address.add(address);
				}
				infoschemaTablesTableSchemaMap.put(varCharValue.data, varCharValue);
			}
			
			//Read Tables Table Name Index file into Map
			while(infoschemaTablesTableNameIndex.getFilePointer() < infoschemaTablesTableNameIndex.length())
			{
				data = "";
				varCharValue = new VarcharIndexClass();
				dataLength = infoschemaTablesTableNameIndex.readByte();
				for(int i = 0; i < dataLength; i++)
				{
					data = data + (char)infoschemaTablesTableNameIndex.readByte();
				}
				varCharValue.data = data;
				repeatCount = infoschemaTablesTableNameIndex.readInt();
				varCharValue.repeatCount = repeatCount;
				for(int i = 0; i < repeatCount; i++)
				{
					address = infoschemaTablesTableNameIndex.readLong();
					varCharValue.address.add(address);
				}
				infoschemaTablesTableNameMap.put(varCharValue.data, varCharValue);
			}
			
			//Read Tables Table Rows Index file into Map
			while(infoschemaTablesTableRowsIndex.getFilePointer() < infoschemaTablesTableRowsIndex.length())
			{
				longValue = new LongIndexClass();
				longValue.data = infoschemaTablesTableRowsIndex.readLong();
				repeatCount = infoschemaTablesTableRowsIndex.readInt();
				longValue.repeatCount = repeatCount;
				for(int i = 0; i < repeatCount; i++)
				{
					address = infoschemaTablesTableRowsIndex.readLong();
					longValue.address.add(address);
				}
				infoschemaTablesTableRowsMap.put(longValue.data, longValue);
			}
			
			//Read Columns Table Schema Index file into Map
			while(infoschemaColumnsTableSchemaIndex.getFilePointer() < infoschemaColumnsTableSchemaIndex.length())
			{
				data = "";
				varCharValue = new VarcharIndexClass();
				dataLength = infoschemaColumnsTableSchemaIndex.readByte();
				for(int i = 0; i < dataLength; i++)
				{
					data = data + (char)infoschemaColumnsTableSchemaIndex.readByte();
				}
				varCharValue.data = data;
				repeatCount = infoschemaColumnsTableSchemaIndex.readInt();
				varCharValue.repeatCount = repeatCount;
				for(int i = 0; i < repeatCount; i++)
				{
					address = infoschemaColumnsTableSchemaIndex.readLong();
					varCharValue.address.add(address);
				}
				infoschemaColumnsTableSchemaMap.put(varCharValue.data, varCharValue);
			}
			
			//Read Columns Table Name Index file into Map
			while(infoschemaColumnsTableNameIndex.getFilePointer() < infoschemaColumnsTableNameIndex.length())
			{
				data = "";
				varCharValue = new VarcharIndexClass();
				dataLength = infoschemaColumnsTableNameIndex.readByte();
				for(int i = 0; i < dataLength; i++)
				{
					data = data + (char)infoschemaColumnsTableNameIndex.readByte();
				}
				varCharValue.data = data;
				repeatCount = infoschemaColumnsTableNameIndex.readInt();
				varCharValue.repeatCount = repeatCount;
				for(int i = 0; i < repeatCount; i++)
				{
					address = infoschemaColumnsTableNameIndex.readLong();
					varCharValue.address.add(address);
				}
				infoschemaColumnsTableNameMap.put(varCharValue.data, varCharValue);
			}
			
			//Read Columns Column Name Index file into Map
			while(infoschemaColumnsColumnNameIndex.getFilePointer() < infoschemaColumnsColumnNameIndex.length())
			{
				data = "";
				varCharValue = new VarcharIndexClass();
				dataLength = infoschemaColumnsColumnNameIndex.readByte();
				for(int i = 0; i < dataLength; i++)
				{
					data = data + (char)infoschemaColumnsColumnNameIndex.readByte();
				}
				varCharValue.data = data;
				repeatCount = infoschemaColumnsColumnNameIndex.readInt();
				varCharValue.repeatCount = repeatCount;
				for(int i = 0; i < repeatCount; i++)
				{
					address = infoschemaColumnsColumnNameIndex.readLong();
					varCharValue.address.add(address);
				}
				infoschemaColumnsColumnNameMap.put(varCharValue.data, varCharValue);
			}
			
			//Read Columns Ordinal Position Index file into Map
			while(infoschemaColumnsOrdinalPositionIndex.getFilePointer() < infoschemaColumnsOrdinalPositionIndex.length())
			{
				intValue = new IntIndexClass();
				intValue.data = infoschemaColumnsOrdinalPositionIndex.readInt();
				repeatCount = infoschemaColumnsOrdinalPositionIndex.readInt();
				intValue.repeatCount = repeatCount;
				for(int i = 0; i < repeatCount; i++)
				{
					address = infoschemaColumnsOrdinalPositionIndex.readLong();
					intValue.address.add(address);
				}
				infoschemaColumnsOrdinalPositionMap.put(intValue.data, intValue);
			}
			
			//Read Columns Column Type Index file into Map
			while(infoschemaColumnsColumnTypeIndex.getFilePointer() < infoschemaColumnsColumnTypeIndex.length())
			{
				data = "";
				varCharValue = new VarcharIndexClass();
				dataLength = infoschemaColumnsColumnTypeIndex.readByte();
				for(int i = 0; i < dataLength; i++)
				{
					data = data + (char)infoschemaColumnsColumnTypeIndex.readByte();
				}
				varCharValue.data = data;
				repeatCount = infoschemaColumnsColumnTypeIndex.readInt();
				varCharValue.repeatCount = repeatCount;
				for(int i = 0; i < repeatCount; i++)
				{
					address = infoschemaColumnsColumnTypeIndex.readLong();
					varCharValue.address.add(address);
				}
				infoschemaColumnsColumnTypeMap.put(varCharValue.data, varCharValue);
			}
			
			//Read Columns Is Nullable Index file into Map
			while(infoschemaColumnsIsNullableIndex.getFilePointer() < infoschemaColumnsIsNullableIndex.length())
			{
				data = "";
				varCharValue = new VarcharIndexClass();
				dataLength = infoschemaColumnsIsNullableIndex.readByte();
				for(int i = 0; i < dataLength; i++)
				{
					data = data + (char)infoschemaColumnsIsNullableIndex.readByte();
				}
				varCharValue.data = data;
				repeatCount = infoschemaColumnsIsNullableIndex.readInt();
				varCharValue.repeatCount = repeatCount;
				for(int i = 0; i < repeatCount; i++)
				{
					address = infoschemaColumnsIsNullableIndex.readLong();
					varCharValue.address.add(address);
				}
				infoschemaColumnsIsNullableMap.put(varCharValue.data, varCharValue);
			}
			
			//Read Columns Column Key Index file into Map
			while(infoschemaColumnsColumnKeyIndex.getFilePointer() < infoschemaColumnsColumnKeyIndex.length())
			{
				data = "";
				varCharValue = new VarcharIndexClass();
				dataLength = infoschemaColumnsColumnKeyIndex.readByte();
				for(int i = 0; i < dataLength; i++)
				{
					data = data + (char)infoschemaColumnsColumnKeyIndex.readByte();
				}
				varCharValue.data = data;
				repeatCount = infoschemaColumnsColumnKeyIndex.readInt();
				varCharValue.repeatCount = repeatCount;
				for(int i = 0; i < repeatCount; i++)
				{
					address = infoschemaColumnsColumnKeyIndex.readLong();
					varCharValue.address.add(address);
				}
				infoschemaColumnsColumnKeyMap.put(varCharValue.data, varCharValue);
			}
			
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}		
	}
	
	/*
	 * Write data into index maps
	 */
	
	public static void writeByteIndexToMap(Byte data, Long address, Map<Byte, ByteIndexClass> ByteMap)
	{
		if(!ByteMap.containsKey(data))
		{
			ByteIndexClass value = new ByteIndexClass();
			value.data = data;
			value.repeatCount = 1;
			value.address.add(address);
			ByteMap.put(data, value);
		}
		else
		{
			ByteMap.get(data).address.add(address);
			ByteMap.get(data).repeatCount += 1;
		}
	}
	public static void writeLongIndexToMap(Long data, Long address, Map<Long, LongIndexClass> longMap)
	{
		if(!longMap.containsKey(data))
		{
			LongIndexClass value = new LongIndexClass();
			value.data = data;
			value.repeatCount = 1;
			value.address.add(address);
			longMap.put(data, value);
		}
		else
		{
			longMap.get(data).address.add(address);
			longMap.get(data).repeatCount += 1; 
		}
	}
	public static void writeIntIndexToMap(Integer data, Long address, Map<Integer, IntIndexClass> IntMap)
	{
		if(!IntMap.containsKey(data))
		{
			IntIndexClass value = new IntIndexClass();
			value.data = data;
			value.repeatCount = 1;
			value.address.add(address);
			IntMap.put(data, value);
		}
		else
		{
			IntMap.get(data).address.add(address);
			IntMap.get(data).repeatCount += 1;
		}
	}
	public static void writeShortIndexToMap(Short data, Long address, Map<Short, ShortIndexClass> ShortMap)
	{
		if(!ShortMap.containsKey(data))
		{
			ShortIndexClass value = new ShortIndexClass();
			value.data = data;
			value.repeatCount = 1;
			value.address.add(address);
			ShortMap.put(data, value);
		}
		else
		{
			ShortMap.get(data).address.add(address);
			ShortMap.get(data).repeatCount += 1;
		}
	}
	public static void writeDateTimeIndexToMap(Long data, Long address, Map<Long, DateTimeIndexClass> dateTimeMap)
	{
		if(!dateTimeMap.containsKey(data))
		{
			DateTimeIndexClass value = new DateTimeIndexClass();
			value.data = data;
			value.repeatCount = 1;
			value.address.add(address);
			dateTimeMap.put(data, value);
		}
		else
		{
			dateTimeMap.get(data).address.add(address);
			dateTimeMap.get(data).repeatCount += 1; 
		}
	}
	public static void writeDateIndexToMap(Long data, Long address, Map<Long, DateIndexClass> dateMap)
	{
		if(!dateMap.containsKey(data))
		{
			DateIndexClass value = new DateIndexClass();
			value.data = data;
			value.repeatCount = 1;
			value.address.add(address);
			dateMap.put(data, value);
		}
		else
		{
			dateMap.get(data).address.add(address);
			dateMap.get(data).repeatCount += 1; 
		}
	}
	public static void writeDoubleIndexToMap(Double data, Long address, Map<Double, DoubleIndexClass> doubleMap)
	{
		if(!doubleMap.containsKey(data))
		{
			DoubleIndexClass value = new DoubleIndexClass();
			value.data = data;
			value.repeatCount = 1;
			value.address.add(address);
			doubleMap.put(data, value);
		}
		else
		{
			doubleMap.get(data).address.add(address);
			doubleMap.get(data).repeatCount += 1; 
		}
	}
	public static void writeFloatIndexToMap(Float data, Long address, Map<Float, FloatIndexClass> floatMap)
	{
		if(!floatMap.containsKey(data))
		{
			FloatIndexClass value = new FloatIndexClass();
			value.data = data;
			value.repeatCount = 1;
			value.address.add(address);
			floatMap.put(data, value);
		}
		else
		{
			floatMap.get(data).address.add(address);
			floatMap.get(data).repeatCount += 1; 
		}
	}
	public static void writeVarcharIndexToMap(String data, Long address, Map<String, VarcharIndexClass> VarcharMap)
	{
		if(!VarcharMap.containsKey(data))
		{
			VarcharIndexClass value = new VarcharIndexClass();
			value.data = data;
			value.repeatCount = 1;
			value.address.add(address);
			VarcharMap.put(data, value);
		}
		else
		{
			VarcharMap.get(data).address.add(address);
			VarcharMap.get(data).repeatCount += 1;
		}
	}
	public static void writeCharIndexToMap(String data, Long address, Map<String, CharIndexClass> charMap)
	{
		if(!charMap.containsKey(data))
		{
			CharIndexClass value = new CharIndexClass();
			value.data = data;
			value.repeatCount = 1;
			value.address.add(address);
			charMap.put(data, value);
		}
		else
		{
			charMap.get(data).address.add(address);
			charMap.get(data).repeatCount += 1;
		}
	}
	
	/*
	 * Write index maps into file
	 */
	public static void writeShortIndexToFile(Map<Short, ShortIndexClass> shortMap, RandomAccessFile file, long Location)
	{
		try
		{
			Set keys = shortMap.keySet();
			Iterator it = keys.iterator();
			file.seek(Location);
			while(it.hasNext())
			{
				ShortIndexClass value = shortMap.get(it.next());
				file.writeShort(value.data);
				file.writeInt(value.repeatCount);
				//Write multiple addresses for multiple occurrances
				for(int i = 0; i < value.repeatCount; i++)
				{
					file.writeLong(value.address.get(i));
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void writeByteIndexToFile(Map<Byte, ByteIndexClass> byteMap, RandomAccessFile file, long Location)
	{
		try
		{
			Set keys = byteMap.keySet();
			Iterator it = keys.iterator();
			file.seek(Location);
			while(it.hasNext())
			{
				ByteIndexClass value = byteMap.get(it.next());
				file.writeByte(value.data);
				file.writeInt(value.repeatCount);
				//Write multiple addresses for multiple occurrances
				for(int i = 0; i < value.repeatCount; i++)
				{
					file.writeLong(value.address.get(i));
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void writeFloatIndexToFile(Map<Float, FloatIndexClass> floatMap, RandomAccessFile file, long Location)
	{
		try
		{
			Set keys = floatMap.keySet();
			Iterator it = keys.iterator();
			file.seek(Location);
			while(it.hasNext())
			{
				FloatIndexClass value = floatMap.get(it.next());
				file.writeFloat(value.data);
				file.writeInt(value.repeatCount);
				//Write multiple addresses for multiple occurrances
				for(int i = 0; i < value.repeatCount; i++)
				{
					file.writeLong(value.address.get(i));
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void writeDoubleIndexToFile(Map<Double, DoubleIndexClass> doubleMap, RandomAccessFile file, long Location)
	{
		try
		{
			Set keys = doubleMap.keySet();
			Iterator it = keys.iterator();
			file.seek(Location);
			while(it.hasNext())
			{
				DoubleIndexClass value = doubleMap.get(it.next());
				file.writeDouble(value.data);
				file.writeInt(value.repeatCount);
				//Write multiple addresses for multiple occurrances
				for(int i = 0; i < value.repeatCount; i++)
				{
					file.writeLong(value.address.get(i));
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void writeDateIndexToFile(Map<Long, DateIndexClass> dateMap, RandomAccessFile file, long Location)
	{
		try
		{
			Set keys = dateMap.keySet();
			Iterator it = keys.iterator();
			file.seek(Location);
			while(it.hasNext())
			{
				DateIndexClass value = dateMap.get(it.next());
				file.writeLong(value.data);
				file.writeInt(value.repeatCount);
				//Write multiple addresses for multiple occurrances
				for(int i = 0; i < value.repeatCount; i++)
				{
					file.writeLong(value.address.get(i));
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void writeDateTimeIndexToFile(Map<Long, DateTimeIndexClass> dateTimeMap, RandomAccessFile file, long Location)
	{
		try
		{
			Set keys = dateTimeMap.keySet();
			Iterator it = keys.iterator();
			file.seek(Location);
			while(it.hasNext())
			{
				DateTimeIndexClass value = dateTimeMap.get(it.next());
				file.writeLong(value.data);
				file.writeInt(value.repeatCount);
				//Write multiple addresses for multiple occurrances
				for(int i = 0; i < value.repeatCount; i++)
				{
					file.writeLong(value.address.get(i));
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void writeLongIndexToFile(Map<Long, LongIndexClass> longMap, RandomAccessFile file, long Location)
	{
		try
		{
			Set keys = longMap.keySet();
			Iterator it = keys.iterator();
			file.seek(Location);
			while(it.hasNext())
			{
				LongIndexClass value = longMap.get(it.next());
				file.writeLong(value.data);
				file.writeInt(value.repeatCount);
				//Write multiple addresses for multiple occurrances
				for(int i = 0; i < value.repeatCount; i++)
				{
					file.writeLong(value.address.get(i));
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void writeIntIndexToFile(Map<Integer, IntIndexClass> IntMap, RandomAccessFile file, long Location)
	{
		try
		{
			Set keys = IntMap.keySet();
			Iterator it = keys.iterator();
			file.seek(Location);
			while(it.hasNext())
			{
				IntIndexClass value = IntMap.get(it.next());
				file.writeInt(value.data);
				file.writeInt(value.repeatCount);
				//Write multiple addresses for multiple occurrances
				for(int i = 0; i < value.repeatCount; i++)
				{
					file.writeLong(value.address.get(i));
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void writeVarcharIndexToFile(Map<String, VarcharIndexClass> VarcharMap, RandomAccessFile file, long Location)
	{
		try
		{
			Set keys = VarcharMap.keySet();
			Iterator it = keys.iterator();
			file.seek(Location);
			while(it.hasNext())
			{
				VarcharIndexClass value = VarcharMap.get(it.next());
				file.writeByte(value.data.length());
				file.writeBytes(value.data);
				file.writeInt(value.repeatCount);
				//Write multiple addresses for multiple occurrances
				for(int i = 0; i < value.repeatCount; i++)
				{
					file.writeLong(value.address.get(i));
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void writeCharIndexToFile(Map<String, CharIndexClass> charMap, RandomAccessFile file, long Location, int size)
	{
		try
		{
			Set keys = charMap.keySet();
			Iterator it = keys.iterator();
			file.seek(Location);
			while(it.hasNext())
			{
				CharIndexClass value = charMap.get(it.next());
				file.writeByte(size);
				file.writeBytes(value.data);
				int count = 0;
				int diff = (size-1) - value.data.length();
				if(diff > 0)
				{
					while(count != diff)
					{
						file.writeByte('\0');
						count++;
					}
				}
				file.writeByte('\n');
				file.writeInt(value.repeatCount);
				//Write multiple addresses for multiple occurrances
				for(int i = 0; i < value.repeatCount; i++)
				{
					file.writeLong(value.address.get(i));
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
