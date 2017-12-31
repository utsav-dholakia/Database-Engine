package src;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Date;

public class Commands {
	
	public static void showAllSchemas()
	{
		try
		{
			RandomAccessFile schemataTableFile = new RandomAccessFile("./Schemas/information_schema/Data/" + Properties.information_schemaSchemataTable, "rw");
			int dataLength = 0;
			long address = 0;
			VarcharIndexClass tempClass = new VarcharIndexClass();
			int count = 1;
			System.out.println("Number | Schema Name");
			Set schemaNameKeys = Schema.infoschemaSchemataSchemaNameMap.keySet();
			Iterator it = schemaNameKeys.iterator();
			
			while(it.hasNext())
			{
				tempClass = Schema.infoschemaSchemataSchemaNameMap.get(it.next());
				System.out.print(count + " | ");
				address = tempClass.address.get(0);
				schemataTableFile.seek(address);
				dataLength = schemataTableFile.readByte();
				for(int i = 0; i < dataLength; i++)
				{
					System.out.print((char)schemataTableFile.readByte());
				}
				System.out.println();
				count++;
			}
			System.out.println();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void showAllTables()
	{
		try
		{
			System.out.println("Current Schema : " + Schema.selectedSchema);
			if(Schema.infoschemaTablesTableSchemaMap.containsKey(Schema.selectedSchema))
			{
				RandomAccessFile tablesTableFile = new RandomAccessFile("./Schemas/information_schema/Data/" + Properties.information_schemaTablesTable, "rw");
				int tableNameDataLength = 0;
				int count = 1;
				System.out.println("Number | Table Name");
				VarcharIndexClass tempClass = new VarcharIndexClass();
				tempClass = Schema.infoschemaTablesTableSchemaMap.get(Schema.selectedSchema);	//Find out schema value map object from current schema
				int repeatCount = tempClass.repeatCount;										//Find how many times it is repeated
				long actualDataAddress;
				long address;
				for(int i = 0; i < repeatCount; i++)											//Loop until all its occurrances
				{
					System.out.print(count + " | ");
					address = tempClass.address.get(i);											//Get schema name address one by one
					tablesTableFile.seek(address);												//Goto that position
					actualDataAddress = address + (long)tablesTableFile.readByte() + 1;				//Actual table name related to that is above address(which is schema name length data) content + the above address
					tablesTableFile.seek(actualDataAddress);									//Goto that position
					tableNameDataLength = tablesTableFile.readByte();							//Read table name data length
					for(int j = 0; j < tableNameDataLength; j++)
					{
						System.out.print((char)tablesTableFile.readByte());						//Print table name character by character
					}
					count++;
					System.out.println();
				}
			}
			else
			{
				System.out.println("No tables in this schema");
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void changeSchema(String schemaName)
	{
		try
		{	
			//Only if schema name map contains the given schema name entry
			if(Schema.infoschemaSchemataSchemaNameMap.containsKey(schemaName))
			{
				RandomAccessFile schemataTableFile = new RandomAccessFile("./Schemas/information_schema/Data/" + Properties.information_schemaSchemataTable, "rw");
				VarcharIndexClass tempClass = new VarcharIndexClass();
				tempClass = Schema.infoschemaSchemataSchemaNameMap.get(schemaName);
				int dataLength = 0;
				long address = tempClass.address.get(0);
				String fetchedSchemaName = "";
				schemataTableFile.seek(address);
				dataLength = schemataTableFile.readByte();
				for(int i = 0; i < dataLength; i++)
				{
					fetchedSchemaName = fetchedSchemaName + (char)schemataTableFile.readByte() ;		//Generate schema name by reading through whole schema names from schemata file
				}
				if(fetchedSchemaName.equals(schemaName))
				{
					System.out.println("CURRENT SCHEMA CHANGED TO " + schemaName);
					Schema.selectedSchema = schemaName;		//Change currently selected schema value
				}
			}
			else
			{	
				System.out.println("Give schema name that exists");
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void createSchema(String schemaName)
	{
		try
		{
			RandomAccessFile tablesTableFile = new RandomAccessFile("./Schemas/information_schema/Data/" + Properties.information_schemaTablesTable, "rw");
			RandomAccessFile schemataTableFile = new RandomAccessFile("./Schemas/information_schema/Data/" + Properties.information_schemaSchemataTable, "rw");
			Schema.writeVarcharIndexToMap(schemaName, schemataTableFile.length(), Schema.infoschemaSchemataSchemaNameMap);
			schemataTableFile.seek(schemataTableFile.length());
			schemataTableFile.writeByte(schemaName.length());
			schemataTableFile.writeBytes(schemaName);
			VarcharIndexClass tempRowsClass = Schema.infoschemaTablesTableNameMap.get("SCHEMATA");
			long schemaNameAddress = tempRowsClass.address.get(0);		//First it will give schema name address (row's starting address)
			tablesTableFile.seek(schemaNameAddress);
			long tableNameAddress = schemaNameAddress + tablesTableFile.readByte() + 1;	//Then generate table name address
			tablesTableFile.seek(tableNameAddress);
			long tableRowsAddress = tableNameAddress + tablesTableFile.readByte() + 1;		//Then generate table rows address
			tablesTableFile.seek(tableRowsAddress);
			long oldData = tablesTableFile.readLong();
			long newData = oldData + (long)1;
			tablesTableFile.seek(tableRowsAddress);				//Overwrite oldData
			tablesTableFile.writeLong(newData);					//Update tables table data file
			System.out.println("Schema created: "+ schemaName);
			File schemaDir = new File("./Schemas/" + schemaName +"/Data");
			schemaDir.mkdirs();
			schemaDir = new File("./Schemas/" + schemaName +"/Index");
			schemaDir.mkdirs();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("Schema could not be created");
		}
	}
	
	public static void createTable(String tableName, Matcher m)
	{
		try
		{
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
			
			//Data file and index file updates in tables table
			tablesTableFile.seek(tablesTableFile.length());
			Schema.writeVarcharIndexToMap(Schema.selectedSchema, tablesTableFile.getFilePointer(), Schema.infoschemaTablesTableSchemaMap);
			Schema.writeVarcharIndexToMap(tableName, tablesTableFile.getFilePointer(), Schema.infoschemaTablesTableNameMap);
			Schema.writeLongIndexToMap((long)0, tablesTableFile.getFilePointer(), Schema.infoschemaTablesTableRowsMap);
			tablesTableFile.writeByte(Schema.selectedSchema.length()); // TABLE_SCHEMA
			tablesTableFile.writeBytes(Schema.selectedSchema);
			tablesTableFile.writeByte(tableName.length()); // TABLE_NAME
			tablesTableFile.writeBytes(tableName);
			tablesTableFile.writeLong(0); // TABLE_ROWS
			
			int countOrdinalPosition = 1;
			//Data files and index file updates in columns tables
			while(m.find( )) 
			{
				long addressForRow = columnsTableFile.length();
				columnsTableFile.seek(addressForRow);
				//write schema name 
				Schema.writeVarcharIndexToMap(Schema.selectedSchema, addressForRow, Schema.infoschemaColumnsTableSchemaMap);
				Schema.writeVarcharIndexToMap(tableName, addressForRow, Schema.infoschemaColumnsTableNameMap);
				Schema.writeVarcharIndexToMap(m.group(1), addressForRow, Schema.infoschemaColumnsColumnNameMap);
				Schema.writeIntIndexToMap(countOrdinalPosition, addressForRow, Schema.infoschemaColumnsOrdinalPositionMap);
				columnsTableFile.writeByte(Schema.selectedSchema.length()); // TABLE_SCHEMA
				columnsTableFile.writeBytes(Schema.selectedSchema);
				//write table name
				columnsTableFile.writeByte(tableName.length()); // TABLE_NAME
				columnsTableFile.writeBytes(tableName);
				//write column name
				//System.out.println("column name: " + m.group(1));
				columnsTableFile.writeByte(m.group(1).length()); // COLUMN_NAME
				columnsTableFile.writeBytes(m.group(1));
				//write ordinal position
				//System.out.println("ordinal position: " + countOrdinalPosition);
				columnsTableFile.writeInt(countOrdinalPosition); // ORDINAL_POSITION
				countOrdinalPosition++;
				//write data type
				if(m.group(2).equals("VARCHAR")||m.group(2).equals("CHAR")||m.group(2).equals("varchar")||m.group(2).equals("char"))
		    	{
		    		//System.out.println("dataType: " + m.group(2).toLowerCase()+"("+m.group(4).toLowerCase()+")");
		    		Schema.writeVarcharIndexToMap(m.group(2).toLowerCase()+"("+m.group(4).toLowerCase()+")", addressForRow, Schema.infoschemaColumnsColumnTypeMap);
					columnsTableFile.writeByte((m.group(2).toLowerCase()+"("+m.group(4).toLowerCase()+")").length()); // COLUMN_TYPE
					columnsTableFile.writeBytes(m.group(2).toLowerCase()+"("+m.group(4).toLowerCase()+")");	
		    	}
				else
				{
					//System.out.println("dataType: " + m.group(2).toLowerCase());
					Schema.writeVarcharIndexToMap(m.group(2).toLowerCase(), addressForRow, Schema.infoschemaColumnsColumnTypeMap);
					columnsTableFile.writeByte(m.group(2).toLowerCase().length()); // COLUMN_TYPE
					columnsTableFile.writeBytes(m.group(2).toLowerCase());
				}
				//write is_nullable value
				if(m.group(5) != null)
		    	 {
		    		 if(m.group(5).equals(" NOT NULL") || m.group(5).equals(" PRIMARY KEY")||m.group(5).equals(" not null") || m.group(5).equals(" primary key"))
		    		 {
		    			//System.out.println("constraint: NOT NULL");
		    			Schema.writeVarcharIndexToMap("NO", addressForRow, Schema.infoschemaColumnsIsNullableMap);
		 				columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
		 				columnsTableFile.writeBytes("NO");
		    		 }
		    		 else
		    		 {
		    			//System.out.println("No constraint");
		    			Schema.writeVarcharIndexToMap("YES", addressForRow, Schema.infoschemaColumnsIsNullableMap);
		 				columnsTableFile.writeByte("YES".length()); // IS_NULLABLE
		 				columnsTableFile.writeBytes("YES");
		    		 }
		    	 }
				//write primary key value
				if(m.group(5) != null)
		    	 {
		    		 if(m.group(5).equals(" PRIMARY KEY")||m.group(5).equals(" primary key"))
		    		 {
		    			//System.out.println("Key :" + m.group(5));
		    			Schema.writeVarcharIndexToMap("PRI", addressForRow, Schema.infoschemaColumnsColumnKeyMap);
		 				columnsTableFile.writeByte("PRI".length()); // IS_NULLABLE
		 				columnsTableFile.writeBytes("PRI");
		    		 }
		    		 else
		    		 {
		    			//System.out.println("No Key"); 
		    			Schema.writeVarcharIndexToMap("", addressForRow, Schema.infoschemaColumnsColumnKeyMap);
		 				columnsTableFile.writeByte("".length()); // IS_NULLABLE
		 				columnsTableFile.writeBytes("");
		    		 }
		    	 }
				
			//Create index file for the column
			RandomAccessFile newIndexFile = new RandomAccessFile("./Schemas/"+Schema.selectedSchema+"/Index/"+Schema.selectedSchema+"."+tableName+"."+m.group(1).toLowerCase()+".ndx", "rw");
			
			//Update COLUMNS table row count
			VarcharIndexClass tempRowsClass = Schema.infoschemaTablesTableNameMap.get("COLUMNS");
			long schemaNameAddress = tempRowsClass.address.get(0);		//First it will give schema name address (row's starting address)
			tablesTableFile.seek(schemaNameAddress);
			long tableNameAddress = schemaNameAddress + tablesTableFile.readByte() + 1;	//Then generate table name address
			tablesTableFile.seek(tableNameAddress);
			long tableRowsAddress = tableNameAddress + tablesTableFile.readByte() + 1;		//Then generate table rows address
			tablesTableFile.seek(tableRowsAddress);
			long oldData = tablesTableFile.readLong();
			long newData = oldData + (long)1;
			tablesTableFile.seek(tableRowsAddress);				//Overwrite oldData
			tablesTableFile.writeLong(newData);	
		   
			}
			//Create table data file
			RandomAccessFile newTableFile = new RandomAccessFile("./Schemas/"+Schema.selectedSchema+"/Data/"+Schema.selectedSchema+"."+tableName+".tbl", "rw");
			System.out.println("Table created: "+ tableName);

			//Update TABLES table row count
			VarcharIndexClass tempRowsClass = Schema.infoschemaTablesTableNameMap.get("TABLES");
			long schemaNameAddress = tempRowsClass.address.get(0);		//First it will give schema name address (row's starting address)
			tablesTableFile.seek(schemaNameAddress);
			long tableNameAddress = schemaNameAddress + tablesTableFile.readByte() + 1;	//Then generate table name address
			tablesTableFile.seek(tableNameAddress);
			long tableRowsAddress = tableNameAddress + tablesTableFile.readByte() + 1;		//Then generate table rows address
			tablesTableFile.seek(tableRowsAddress);
			long oldData = tablesTableFile.readLong();
			long newData = oldData + (long)1;
			tablesTableFile.seek(tableRowsAddress);				//Overwrite oldData
			tablesTableFile.writeLong(newData);					//Update tables table data file

		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("Table could not be created");
		}
	}
	
	public static void insert(String tableName, String[] values)
	{
		try
		{
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
			
			//Create RandomAccessFile object for the table
			RandomAccessFile tempTableFile = new RandomAccessFile("./Schemas/"+Schema.selectedSchema+"/Data/"+Schema.selectedSchema+"."+tableName+".tbl", "rw");
			
			File indexDir = new File ("./Schemas/"+Schema.selectedSchema+"/Index/");		//Get index Directory file pointer
			File[] files = indexDir.listFiles();											//List all files in that dir
			RandomAccessFile actualIndexFile = new RandomAccessFile(files[0].getPath(),"rw");
			
			//If table name given is wrong
			if(!Schema.infoschemaColumnsTableNameMap.containsKey(tableName))
			{
				System.out.println("Table Name wrong");
				return;
			}
			//Get column types for given table
			VarcharIndexClass tableNameClass = Schema.infoschemaColumnsTableNameMap.get(tableName);
			long schemaNameAddress;
			long tableNameAddress;
			long actualAddress;
			long columnNameAddress;
			long ordinalPositionAddress;
			int dataTypeLength;
			long addressForRow = tempTableFile.length();
			long tableRowsAddress;
			for(int i = 0; i < tableNameClass.address.size(); i++)		//Get data type one by one and write into table data file accordingly
			{
				//System.out.println("Index :" + i);
				String dataType = "";
				String columnName = "";
				schemaNameAddress = tableNameClass.address.get(i);
				columnsTableFile.seek(schemaNameAddress);
				tableNameAddress = schemaNameAddress + columnsTableFile.readByte() + 1;
				columnsTableFile.seek(tableNameAddress);
				columnNameAddress = tableNameAddress + columnsTableFile.readByte() + 1;
				columnsTableFile.seek(columnNameAddress);
				
				//Find column Name
				int columnNameLength = columnsTableFile.readByte();
				for(int k = 0; k < columnNameLength; k++)
				{
					columnName = columnName + (char)columnsTableFile.readByte();
				}
				//System.out.println("columnName :"+columnName);
				//Search for index file in the list of index files
				for(int x = 0; x < files.length; x++)
				{
					if(files[x].getPath().endsWith(columnName.toLowerCase()+".ndx"))
					{
						actualIndexFile = new RandomAccessFile(files[x].getPath(),"rw");	//assign object the correct file pointer that is the related index file
						break;
					}
				}
				columnsTableFile.readInt();
				actualAddress = columnsTableFile.getFilePointer();
				dataTypeLength = columnsTableFile.readByte();
				//System.out.println("dataType length :" + dataTypeLength);
				for(int j = 0; j < dataTypeLength; j++)
				{
					dataType = dataType + (char)columnsTableFile.readByte();
				}
				//System.out.println("before dataType :" + dataType);
				//Find out actual datatype name and size associated with it (if any)
				String regex = "(\\w+)(\\d+)?";
				Pattern pattern = Pattern.compile(regex);
				Matcher match = pattern.matcher(dataType);
				int dataSize = 0;
				int count = 0;
				while(match.find())
				{
					if(count == 0)
					dataType = match.group(0);
					else
					dataSize = Integer.parseInt(match.group(0));	
					count++;
				}
				//System.out.println("dataType :" + dataType);
				//System.out.println("dataSize :" + dataSize);
				
				tempTableFile.seek(tempTableFile.length()); 		//Point to the end of table data file
				if(dataType.equals("int"))	//If datatype found is INT type
				{
					TreeMap<Integer, IntIndexClass> tempMap = new TreeMap<Integer, IntIndexClass>();	//create temp int type map for index
					Schema.writeIntIndexToMap(Integer.parseInt(values[i]), addressForRow, tempMap);	//write into map (value,current file location,map created above)
					tempTableFile.writeInt(Integer.parseInt(values[i]));		//write INT type data into actual data file
					Schema.writeIntIndexToFile(tempMap, actualIndexFile, actualIndexFile.length());		//write index information into index file from map
				}
				else if(dataType.equals("byte"))	//If datatype found is BYTE type
				{
					TreeMap<Byte, ByteIndexClass> tempMap = new TreeMap<Byte, ByteIndexClass>();
					Schema.writeByteIndexToMap(Byte.parseByte(values[i]), addressForRow, tempMap);
					tempTableFile.writeByte(Byte.parseByte(values[i]));
					Schema.writeByteIndexToFile(tempMap, actualIndexFile, actualIndexFile.length());
				}
				else if(dataType.equals("short"))	//If datatype found is SHORT type
				{
					TreeMap<Short, ShortIndexClass> tempMap = new TreeMap<Short, ShortIndexClass>();
					Schema.writeShortIndexToMap(Short.parseShort(values[i]), addressForRow, tempMap);
					tempTableFile.writeShort(Short.parseShort(values[i]));
					Schema.writeShortIndexToFile(tempMap, actualIndexFile, actualIndexFile.length());
				}	
				else if(dataType.equals("long"))	//If datatype found is LONG type
				{
					TreeMap<Long, LongIndexClass> tempMap = new TreeMap<Long, LongIndexClass>();
					Schema.writeLongIndexToMap(Long.parseLong(values[i]), addressForRow, tempMap);
					tempTableFile.writeLong(Long.parseLong(values[i]));
					Schema.writeLongIndexToFile(tempMap, actualIndexFile, actualIndexFile.length());
				}
				else if(dataType.equals("char"))	//If datatype found is CHAR type
				{
					TreeMap<String, CharIndexClass> tempMap = new TreeMap<String, CharIndexClass>();
					Schema.writeCharIndexToMap(values[i], addressForRow, tempMap);
					tempTableFile.writeByte(values[i].length());
					tempTableFile.writeBytes(values[i]);
					//Write '\n' after data and then pad remaining space with '\0'
					int writeCount = values[i].length();
					tempTableFile.writeByte('\n');
					writeCount++;
					while(writeCount < dataSize - 1)
					{
						tempTableFile.writeByte('\0');
						writeCount++;
					}
					Schema.writeCharIndexToFile(tempMap, actualIndexFile, actualIndexFile.length(), dataSize);
				}
				else if(dataType.equals("varchar"))	//If datatype found is VARCHAR type
				{
					TreeMap<String, VarcharIndexClass> tempMap = new TreeMap<String, VarcharIndexClass>();
					Schema.writeVarcharIndexToMap(values[i], addressForRow, tempMap);
					tempTableFile.writeByte(values[i].length());
					tempTableFile.writeBytes(values[i]);
					Schema.writeVarcharIndexToFile(tempMap, actualIndexFile, actualIndexFile.length());
				}
				else if(dataType.equals("float"))	//If datatype found is FLOAT type
				{
					TreeMap<Float, FloatIndexClass> tempMap = new TreeMap<Float, FloatIndexClass>();
					Schema.writeFloatIndexToMap(Float.parseFloat(values[i]), addressForRow, tempMap);
					tempTableFile.writeFloat(Float.parseFloat(values[i]));
					Schema.writeFloatIndexToFile(tempMap, actualIndexFile, actualIndexFile.length());
				}
				else if(dataType.equals("double"))	//If datatype found is DOUBLE type
				{
					TreeMap<Double, DoubleIndexClass> tempMap = new TreeMap<Double, DoubleIndexClass>();
					Schema.writeDoubleIndexToMap(Double.parseDouble(values[i]), addressForRow, tempMap);
					tempTableFile.writeDouble(Double.parseDouble(values[i]));
					Schema.writeDoubleIndexToFile(tempMap, actualIndexFile, actualIndexFile.length());
				}
				else if(dataType.equals("date"))	//If datatype found is DATE type
				{
					String dateInput = values[i];
				    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				    Date date = df.parse(dateInput);
				    long epoch = date.getTime();		//This contains long epoch data coverted from date string
				   
					TreeMap<Long, DateIndexClass> tempMap = new TreeMap<Long, DateIndexClass>();
					Schema.writeDateIndexToMap(epoch, addressForRow, tempMap);
					tempTableFile.writeLong(epoch);
					Schema.writeDateIndexToFile(tempMap, actualIndexFile, actualIndexFile.length());
				}
				else if(dataType.equals("datetime"))	//If datatype found is DATETIME type
				{
					String dateInput = values[i];
				    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
				    Date date = df.parse(dateInput);
				    long epoch = date.getTime();		//This contains long epoch data coverted from date string
				   
					TreeMap<Long, DateIndexClass> tempMap = new TreeMap<Long, DateIndexClass>();
					Schema.writeDateIndexToMap(epoch, addressForRow, tempMap);
					tempTableFile.writeLong(epoch);
					Schema.writeDateIndexToFile(tempMap, actualIndexFile, actualIndexFile.length());
				}
			}
			System.out.println("Row inserted");
			VarcharIndexClass tempRowsClass = Schema.infoschemaTablesTableNameMap.get(tableName);
			schemaNameAddress = tempRowsClass.address.get(0);		//First it will give schema name address (row's starting address)
			tablesTableFile.seek(schemaNameAddress);
			tableNameAddress = schemaNameAddress + tablesTableFile.readByte() + 1;	//Then generate table name address
			tablesTableFile.seek(tableNameAddress);
			tableRowsAddress = tableNameAddress + tablesTableFile.readByte() + 1;		//Then generate table rows address
			tablesTableFile.seek(tableRowsAddress);
			long oldData = tablesTableFile.readLong();
			long newData = oldData + (long)1;
			tablesTableFile.seek(tableRowsAddress);				//Overwrite oldData
			tablesTableFile.writeLong(newData);					//Update tables table data file
			//Update tables table table rows index file below
			/*
			LongIndexClass tempClass = Schema.infoschemaTablesTableRowsMap.get(oldData);
			if(tempClass.repeatCount == 1)
			{
				Schema.infoschemaTablesTableRowsMap.remove(oldData);	//Remove old data entry object completely
			//	System.out.println("Remove old index entry for :" + oldData);
			}
			else
			{
				Schema.infoschemaTablesTableRowsMap.get(oldData).address.remove(schemaNameAddress);	//Add address of the old data entry from index file
				Schema.infoschemaTablesTableRowsMap.get(oldData).repeatCount--;
			}
			Schema.writeLongIndexToMap(newData, schemaNameAddress, Schema.infoschemaTablesTableRowsMap);
			Schema.writeLongIndexToFile(Schema.infoschemaTablesTableRowsMap, infoschemaTablesTableRowsIndex, 0);
			*/
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
	public static void select(String tableName)
	{
		try
		{
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
			
			//Create RandomAccessFile object for the table
			RandomAccessFile tempTableFile = new RandomAccessFile("./Schemas/"+Schema.selectedSchema+"/Data/"+Schema.selectedSchema+"."+tableName+".tbl", "rw");
			
			File indexDir = new File ("./Schemas/"+Schema.selectedSchema+"/Index/");		//Get index Directory file pointer
			File[] files = indexDir.listFiles();											//List all files in that dir
			RandomAccessFile actualIndexFile = new RandomAccessFile(files[0].getPath(),"rw");
			
			//If table name given is wrong
			if(!Schema.infoschemaColumnsTableNameMap.containsKey(tableName))
			{
				System.out.println("Table Name wrong");
				return;
			}
			long schemaNameAddress;
			long tableNameAddress;
			long tableRowsAddress;
			long columnNameAddress;
			long columnTypeAddress;
			int dataTypeLength;
					
			//Print column Names
			//Get column types for given table
			VarcharIndexClass columnsTableNameClass = Schema.infoschemaColumnsTableNameMap.get(tableName);
			for(int i = 0; i < columnsTableNameClass.address.size(); i++)		//Get data type one by one and read from table data file accordingly
			{
				//System.out.println("Index :" + i);
				String dataType = "";
				String columnName = "";
				schemaNameAddress = columnsTableNameClass.address.get(i);
				columnsTableFile.seek(schemaNameAddress);
				tableNameAddress = schemaNameAddress + columnsTableFile.readByte() + 1;
				columnsTableFile.seek(tableNameAddress);
				columnNameAddress = tableNameAddress + columnsTableFile.readByte() + 1;
				columnsTableFile.seek(columnNameAddress);
				
				//Find column Name
				int columnNameLength = columnsTableFile.readByte();
				for(int k = 0; k < columnNameLength; k++)
				{
					columnName = columnName + (char)columnsTableFile.readByte();
				}
				
				System.out.print(columnName + " | ");			//Print column names
			}

			//Find out how many rows are there in the table
			VarcharIndexClass tempRowsClass = Schema.infoschemaTablesTableNameMap.get(tableName);
			schemaNameAddress = tempRowsClass.address.get(0);		//First it will give schema name address (row's starting address)
			tablesTableFile.seek(schemaNameAddress);
			tableNameAddress = schemaNameAddress + tablesTableFile.readByte() + 1;	//Then generate table name address
			tablesTableFile.seek(tableNameAddress);
			tableRowsAddress = tableNameAddress + tablesTableFile.readByte() + 1;		//Then generate table rows address
			tablesTableFile.seek(tableRowsAddress);
			long rowCount = tablesTableFile.readLong();
			
			//Get column types for given table
			VarcharIndexClass tableNameClass = Schema.infoschemaColumnsTableNameMap.get(tableName);

			for(long rows = 0; rows < rowCount; rows++)
			{
				System.out.println();
				for(int i = 0; i < tableNameClass.address.size(); i++)		//Get data type one by one and read from table data file accordingly
				{
					//System.out.println("Index :" + i);
					String dataType = "";
					schemaNameAddress = tableNameClass.address.get(i);
					columnsTableFile.seek(schemaNameAddress);
					tableNameAddress = schemaNameAddress + columnsTableFile.readByte() + 1;
					columnsTableFile.seek(tableNameAddress);
					columnNameAddress = tableNameAddress + columnsTableFile.readByte() + 1;
					columnsTableFile.seek(columnNameAddress);
					long columnOrdinalPositionAddress = columnNameAddress + columnsTableFile.readByte() + 1; 
					columnsTableFile.seek(columnOrdinalPositionAddress);
					
					columnsTableFile.readInt();
					columnTypeAddress = columnsTableFile.getFilePointer();
					dataTypeLength = columnsTableFile.readByte();
					//System.out.println("dataType length :" + dataTypeLength);
					for(int j = 0; j < dataTypeLength; j++)
					{
						dataType = dataType + (char)columnsTableFile.readByte();
					}
					//System.out.println("before dataType :" + dataType);
					//Find out actual datatype name and size associated with it (if any)
					String regex = "(\\w+)(\\d+)?";
					Pattern pattern = Pattern.compile(regex);
					Matcher match = pattern.matcher(dataType);
					int dataSize = 0;
					int count = 0;
					while(match.find())
					{
						if(count == 0)
						dataType = match.group(0);
						else
						dataSize = Integer.parseInt(match.group(0));	
						count++;
					}
					//System.out.println("dataType :" + dataType);
					//System.out.println("dataSize :" + dataSize);
					
					//tempTableFile.seek(0); 		//Point to the start of table data file
					//Read and print data from data table according to data type of the column
					
					if(dataType.equals("int"))	//If datatype found is INT type
					{
						System.out.print(tempTableFile.readInt() + " | ");		//read INT type data
					}
					else if(dataType.equals("byte"))	//If datatype found is BYTE type
					{
						System.out.print(tempTableFile.readByte() + " | ");		//read Byte type data
					}
					else if(dataType.equals("short"))	//If datatype found is SHORT type
					{
						System.out.print(tempTableFile.readShort() + " | ");		//read Short type data
					}	
					else if(dataType.equals("long"))	//If datatype found is LONG type
					{
						System.out.print(tempTableFile.readLong() + " | ");		//read Long type data
					}
					else if(dataType.equals("char"))	//If datatype found is CHAR type
					{
						int dataLength = tempTableFile.readByte();
						int readCount = 0;
						while(readCount < (dataSize-1))
						{
							char c = (char)tempTableFile.readByte();
							if(readCount < dataLength)
								System.out.print(c);
							
							readCount++;
						}
						/*while(readCount < dataSize)
						{
							tempTableFile.readByte();
							readCount++;
						}*/
						System.out.print(" | ");
					}
					else if(dataType.equals("varchar"))	//If datatype found is VARCHAR type
					{
						int dataLength = tempTableFile.readByte();
						int readCount = 0;
						while(readCount < dataLength)
						{
							System.out.print((char)tempTableFile.readByte());
							readCount++;
						}
						System.out.print(" | ");
					}
					else if(dataType.equals("float"))	//If datatype found is FLOAT type
					{
						System.out.print(tempTableFile.readFloat() + " | ");		//read Float type data
					}
					else if(dataType.equals("double"))	//If datatype found is DOUBLE type
					{
						System.out.print(tempTableFile.readDouble() + " | ");		//read Double type data
					}
					else if(dataType.equals("date"))	//If datatype found is DATE type
					{
						long epoch = tempTableFile.readLong();		//This contains long epoch data coverted from date string
					    Date date = new Date(epoch);
					    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					    String dateOutPut = sdf.format(date);
					    System.out.print(dateOutPut + " | ");
					}
					else if(dataType.equals("datetime"))	//If datatype found is DATETIME type
					{
						long epoch = tempTableFile.readLong();		//This contains long epoch data coverted from datetime string
					    Date date = new Date(epoch);
					    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
					    String dateTimeOutPut = sdf.format(date);
					    System.out.print(dateTimeOutPut + " | ");
					}
				}
			}
			System.out.println();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void selectWhere(String tableName, String columnName, String operator, String value)
	{
		try
		{
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
			
			//Create RandomAccessFile object for the table
			RandomAccessFile tempTableFile = new RandomAccessFile("./Schemas/"+Schema.selectedSchema+"/Data/"+Schema.selectedSchema+"."+tableName+".tbl", "rw");
			
			File indexDir = new File ("./Schemas/"+Schema.selectedSchema+"/Index/");		//Get index Directory file pointer
			File[] files = indexDir.listFiles();											//List all files in that dir
			RandomAccessFile actualIndexFile = new RandomAccessFile(files[0].getPath(),"rw");
			ArrayList<Long> lessThanAddressList = new ArrayList<Long>(),greaterThanAddressList = new ArrayList<Long>(),equalAddressList = new ArrayList<Long>();
			//If table name given is wrong
			if(!Schema.infoschemaColumnsTableNameMap.containsKey(tableName))
			{
				System.out.println("Table Name wrong");
				return;
			}
			long schemaNameAddress;
			long tableNameAddress;
			long tableRowsAddress;
			long columnNameAddress;
			long columnTypeAddress;
			int dataTypeLength;
					
			//Print column Names
			VarcharIndexClass columnsTableNameClass = Schema.infoschemaColumnsTableNameMap.get(tableName);
			for(int i = 0; i < columnsTableNameClass.address.size(); i++)		//Get data type one by one and read from table data file accordingly
			{
				String dataType = "";
				String columnNamePrint = "";
				schemaNameAddress = columnsTableNameClass.address.get(i);
				columnsTableFile.seek(schemaNameAddress);
				tableNameAddress = schemaNameAddress + columnsTableFile.readByte() + 1;
				columnsTableFile.seek(tableNameAddress);
				columnNameAddress = tableNameAddress + columnsTableFile.readByte() + 1;
				columnsTableFile.seek(columnNameAddress);
				
				//Find column Name
				int columnNameLength = columnsTableFile.readByte();
				for(int k = 0; k < columnNameLength; k++)
				{
					columnNamePrint = columnNamePrint + (char)columnsTableFile.readByte();
				}
				
				System.out.print(columnNamePrint + " | ");			//Print column names
			}
			//System.out.println("operator :" + operator);
			//System.out.println("value :" + value);
			//Find out how many rows are there in the table
			VarcharIndexClass tempRowsClass = Schema.infoschemaTablesTableNameMap.get(tableName);
			schemaNameAddress = tempRowsClass.address.get(0);		//First it will give schema name address (row's starting address)
			tablesTableFile.seek(schemaNameAddress);
			tableNameAddress = schemaNameAddress + tablesTableFile.readByte() + 1;	//Then generate table name address
			tablesTableFile.seek(tableNameAddress);
			tableRowsAddress = tableNameAddress + tablesTableFile.readByte() + 1;		//Then generate table rows address
			tablesTableFile.seek(tableRowsAddress);
			long rowCount = tablesTableFile.readLong();
			//System.out.println("row count: " + rowCount);
			//Get given columnName's datatype
			VarcharIndexClass tableNameClass = Schema.infoschemaColumnsTableNameMap.get(tableName);
			for(int i = 0; i < tableNameClass.repeatCount; i++)		//Get data type one by one and read from table data file accordingly
			{
				String dataType = "";
				schemaNameAddress = tableNameClass.address.get(i);
				columnsTableFile.seek(schemaNameAddress);
				tableNameAddress = schemaNameAddress + columnsTableFile.readByte() + 1;
				columnsTableFile.seek(tableNameAddress);
				columnNameAddress = tableNameAddress + columnsTableFile.readByte() + 1;
				columnsTableFile.seek(columnNameAddress);
				//Find column Name
				String columnNameFind = "";
				int columnNameLength = columnsTableFile.readByte();
				for(int k = 0; k < columnNameLength; k++)
				{
					columnNameFind = columnNameFind + (char)columnsTableFile.readByte();
				}
				if(columnNameFind.equals(columnName))	//If found column name matches query's column name
				{
					//System.out.println("selected column Name : " + columnNameFind);
					//Search for index file in the list of index files
					for(int x = 0; x < files.length; x++)
					{
						if(files[x].getPath().endsWith(columnName.toLowerCase()+".ndx"))
						{
							actualIndexFile = new RandomAccessFile(files[x].getPath(),"rw");	//assign object the correct file pointer that is the related index file
							break;
						}
					}
					columnsTableFile.readInt();
					columnTypeAddress = columnsTableFile.getFilePointer();
					dataTypeLength = columnsTableFile.readByte();
					//System.out.println("dataType length :" + dataTypeLength);
					for(int j = 0; j < dataTypeLength; j++)
					{
						dataType = dataType + (char)columnsTableFile.readByte();
					}
					//System.out.println("before dataType :" + dataType);
					//Find out actual datatype name and size associated with it (if any)
					String regex = "(\\w+)(\\d+)?";
					Pattern pattern = Pattern.compile(regex);
					Matcher match = pattern.matcher(dataType);
					int dataSize = 0;
					int count = 0;
					while(match.find())
					{
						if(count == 0)
						dataType = match.group(0);
						else
						dataSize = Integer.parseInt(match.group(0));	
						count++;
					}
					//System.out.println("given dataType :" + dataType);
					//System.out.println("given dataSize :" + dataSize);
					
					long columnOrdinalPositionAddress = columnNameAddress + columnsTableFile.readByte() + 1; 
					columnsTableFile.seek(columnOrdinalPositionAddress);
					
					int repeatCount;
					if(dataType.equals("int"))	//If datatype found is INT type
					{
						long address;
						IntIndexClass tempClass = new IntIndexClass();
						TreeMap<Integer, IntIndexClass> tempMap = new TreeMap<Integer, IntIndexClass>();	//create temp int type map for index
						
						while(actualIndexFile.getFilePointer() < actualIndexFile.length())
						{
							tempClass.data = actualIndexFile.readInt();
							repeatCount = actualIndexFile.readInt();
							int loopcount = 0;
							while(loopcount < repeatCount)
							{
								address = actualIndexFile.readLong();
								Schema.writeIntIndexToMap(tempClass.data,address,tempMap);	//write into map (value,current file location,map created above)
								loopcount++;
							}
						}
						Set keys = tempMap.keySet();
						Iterator it = keys.iterator();
						while(it.hasNext())
						{
							int loopcount = 0;
							tempClass = tempMap.get(it.next());
							if(tempClass.data < Integer.parseInt(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									lessThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data > Integer.parseInt(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									greaterThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data == Integer.parseInt(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									equalAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
						}	
												
					}
					else if(dataType.equals("byte"))	//If datatype found is BYTE type
					{
						long address;
						ByteIndexClass tempClass = new ByteIndexClass();
						TreeMap<Byte, ByteIndexClass> tempMap = new TreeMap<Byte, ByteIndexClass>();	//create temp int type map for index
						
						while(actualIndexFile.getFilePointer() < actualIndexFile.length())
						{
							tempClass.data = actualIndexFile.readByte();
							repeatCount = actualIndexFile.readInt();
							int loopcount = 0;
							while(loopcount < repeatCount)
							{
								address = actualIndexFile.readLong();
								Schema.writeByteIndexToMap(tempClass.data,address,tempMap);	//write into map (value,current file location,map created above)
								loopcount++;
							}
						}
						Set keys = tempMap.keySet();
						Iterator it = keys.iterator();
						while(it.hasNext())
						{
							int loopcount = 0;
							tempClass = tempMap.get(it.next());
							if(tempClass.data < Byte.parseByte(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									lessThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data > Byte.parseByte(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									greaterThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data == Byte.parseByte(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									equalAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
						}
					}
					else if(dataType.equals("short"))	//If datatype found is SHORT type
					{
						long address;
						ShortIndexClass tempClass = new ShortIndexClass();
						TreeMap<Short, ShortIndexClass> tempMap = new TreeMap<Short, ShortIndexClass>();	//create temp int type map for index
						
						while(actualIndexFile.getFilePointer() < actualIndexFile.length())
						{
							tempClass.data = actualIndexFile.readShort();
							repeatCount = actualIndexFile.readInt();
							int loopcount = 0;
							while(loopcount < repeatCount)
							{
								address = actualIndexFile.readLong();
								Schema.writeShortIndexToMap(tempClass.data,address,tempMap);	//write into map (value,current file location,map created above)
								loopcount++;
							}
						}
						Set keys = tempMap.keySet();
						Iterator it = keys.iterator();
						while(it.hasNext())
						{
							int loopcount = 0;
							tempClass = tempMap.get(it.next());
							if(tempClass.data < Short.parseShort(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									lessThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data > Short.parseShort(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									greaterThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data == Short.parseShort(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									equalAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
						}
					}	
					else if(dataType.equals("long"))	//If datatype found is LONG type
					{
						long address;
						LongIndexClass tempClass = new LongIndexClass();
						TreeMap<Long, LongIndexClass> tempMap = new TreeMap<Long, LongIndexClass>();	//create temp int type map for index
						
						while(actualIndexFile.getFilePointer() < actualIndexFile.length())
						{
							tempClass.data = actualIndexFile.readLong();
							repeatCount = actualIndexFile.readInt();
							int loopcount = 0;
							while(loopcount < repeatCount)
							{
								address = actualIndexFile.readLong();
								Schema.writeLongIndexToMap(tempClass.data,address,tempMap);	//write into map (value,current file location,map created above)
								loopcount++;
							}
						}
						Set keys = tempMap.keySet();
						Iterator it = keys.iterator();
						while(it.hasNext())
						{
							int loopcount = 0;
							tempClass = tempMap.get(it.next());
							if(tempClass.data < Long.parseLong(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									lessThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data > Long.parseLong(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									greaterThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data == Long.parseLong(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									equalAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
						}
					}
					else if(dataType.equals("char"))	//If datatype found is CHAR type
					{
						long address;
						CharIndexClass tempClass = new CharIndexClass();
						TreeMap<String, CharIndexClass> tempMap = new TreeMap<String, CharIndexClass>();	//create temp int type map for index
						
						while(actualIndexFile.getFilePointer() < actualIndexFile.length())
						{
							int dataLength = actualIndexFile.readByte();
							int readCount = 0;
							tempClass.data = "";
							while(readCount < dataSize)
							{
								if(readCount < dataLength)
									tempClass.data = tempClass.data + (char)actualIndexFile.readByte();
								else
									actualIndexFile.readByte();
								readCount++;
							}
							repeatCount = actualIndexFile.readInt();
							int loopcount = 0;
							while(loopcount < repeatCount)
							{
								address = actualIndexFile.readLong();
								Schema.writeCharIndexToMap(tempClass.data,address,tempMap);	//write into map (value,current file location,map created above)
								loopcount++;
							}
						}
						Set keys = tempMap.keySet();
						Iterator it = keys.iterator();
						while(it.hasNext())
						{
							int loopcount = 0;
							tempClass = tempMap.get(it.next());
							if(tempClass.data.compareTo(value) < 0)
							{
								while(loopcount < tempClass.repeatCount)
								{
									lessThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data.compareTo(value) > 0)
							{
								while(loopcount < tempClass.repeatCount)
								{
									greaterThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data.equals(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									equalAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
						}
					}
					else if(dataType.equals("varchar"))	//If datatype found is VARCHAR type
					{
						long address;
						VarcharIndexClass tempClass = new VarcharIndexClass();
						TreeMap<String, VarcharIndexClass> tempMap = new TreeMap<String, VarcharIndexClass>();	//create temp int type map for index
						
						while(actualIndexFile.getFilePointer() < actualIndexFile.length())
						{
							int dataLength = actualIndexFile.readByte();
							int readCount = 0;
							tempClass.data = "";
							while(readCount < dataLength)
							{
								tempClass.data = tempClass.data + (char)actualIndexFile.readByte();
								readCount++;
							}
							repeatCount = actualIndexFile.readInt();
							int loopcount = 0;
							while(loopcount < repeatCount)
							{
								address = actualIndexFile.readLong();
								Schema.writeVarcharIndexToMap(tempClass.data,address,tempMap);	//write into map (value,current file location,map created above)
								loopcount++;
							}
						}
						Set keys = tempMap.keySet();
						Iterator it = keys.iterator();
						while(it.hasNext())
						{
							int loopcount = 0;
							tempClass = tempMap.get(it.next());
							if(tempClass.data.equals(value.toString()))
							{
								while(loopcount < tempClass.repeatCount)
								{
									equalAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data.compareTo(value.toString()) < 0)
							{
								while(loopcount < tempClass.repeatCount)
								{
									lessThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data.compareTo(value.toString()) > 0)
							{
								while(loopcount < tempClass.repeatCount)
								{
									greaterThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
						}
					}
					else if(dataType.equals("float"))	//If datatype found is FLOAT type
					{
						long address;
						FloatIndexClass tempClass = new FloatIndexClass();
						TreeMap<Float, FloatIndexClass> tempMap = new TreeMap<Float, FloatIndexClass>();	//create temp int type map for index
						
						while(actualIndexFile.getFilePointer() < actualIndexFile.length())
						{
							tempClass.data = actualIndexFile.readFloat();
							repeatCount = actualIndexFile.readInt();
							int loopcount = 0;
							while(loopcount < repeatCount)
							{
								address = actualIndexFile.readLong();
								Schema.writeFloatIndexToMap(tempClass.data,address,tempMap);	//write into map (value,current file location,map created above)
								loopcount++;
							}
						}
						Set keys = tempMap.keySet();
						Iterator it = keys.iterator();
						while(it.hasNext())
						{
							int loopcount = 0;
							tempClass = tempMap.get(it.next());
							if(tempClass.data < Float.parseFloat(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									lessThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data > Float.parseFloat(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									greaterThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data == Float.parseFloat(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									equalAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
						}
					}
					else if(dataType.equals("double"))	//If datatype found is DOUBLE type
					{
						long address;
						DoubleIndexClass tempClass = new DoubleIndexClass();
						TreeMap<Double, DoubleIndexClass> tempMap = new TreeMap<Double, DoubleIndexClass>();	//create temp int type map for index
						
						while(actualIndexFile.getFilePointer() < actualIndexFile.length())
						{
							tempClass.data = actualIndexFile.readDouble();
							repeatCount = actualIndexFile.readInt();
							int loopcount = 0;
							while(loopcount < repeatCount)
							{
								address = actualIndexFile.readLong();
								Schema.writeDoubleIndexToMap(tempClass.data,address,tempMap);	//write into map (value,current file location,map created above)
								loopcount++;
							}
						}
						Set keys = tempMap.keySet();
						Iterator it = keys.iterator();
						while(it.hasNext())
						{
							int loopcount = 0;
							tempClass = tempMap.get(it.next());
							if(tempClass.data < Double.parseDouble(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									lessThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data > Double.parseDouble(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									greaterThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data == Double.parseDouble(value))
							{
								while(loopcount < tempClass.repeatCount)
								{
									equalAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
						}
					}
					else if(dataType.equals("date"))	//If datatype found is DATE type
					{
						long address;
						DateIndexClass tempClass = new DateIndexClass();
						TreeMap<Long, DateIndexClass> tempMap = new TreeMap<Long, DateIndexClass>();	//create temp int type map for index
						
						while(actualIndexFile.getFilePointer() < actualIndexFile.length())
						{
							tempClass.data = actualIndexFile.readLong();
							repeatCount = actualIndexFile.readInt();
							int loopcount = 0;
							while(loopcount < repeatCount)
							{
								address = actualIndexFile.readLong();
								Schema.writeDateIndexToMap(tempClass.data,address,tempMap);	//write into map (value,current file location,map created above)
								loopcount++;
							}
						}
						Set keys = tempMap.keySet();
						Iterator it = keys.iterator();
						String dateInput = value;
					    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
					    Date date = df.parse(dateInput);
					    long epoch = date.getTime();
					    
						while(it.hasNext())
						{
							int loopcount = 0;
							tempClass = tempMap.get(it.next());
							if(tempClass.data < epoch)
							{
								while(loopcount < tempClass.repeatCount)
								{
									lessThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data > epoch)
							{
								while(loopcount < tempClass.repeatCount)
								{
									greaterThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data == epoch)
							{
								while(loopcount < tempClass.repeatCount)
								{
									equalAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
						}
					}
					else if(dataType.equals("datetime"))	//If datatype found is DATETIME type
					{
						long address;
						DateTimeIndexClass tempClass = new DateTimeIndexClass();
						TreeMap<Long, DateTimeIndexClass> tempMap = new TreeMap<Long, DateTimeIndexClass>();	//create temp int type map for index
						
						while(actualIndexFile.getFilePointer() < actualIndexFile.length())
						{
							tempClass.data = actualIndexFile.readLong();
							repeatCount = actualIndexFile.readInt();
							int loopcount = 0;
							while(loopcount < repeatCount)
							{
								address = actualIndexFile.readLong();
								Schema.writeDateTimeIndexToMap(tempClass.data,address,tempMap);	//write into map (value,current file location,map created above)
								loopcount++;
							}
						}
						Set keys = tempMap.keySet();
						Iterator it = keys.iterator();
						String dateInput = value;
						SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
					    Date date = df.parse(dateInput);
					    long epoch = date.getTime();
					    
						while(it.hasNext())
						{
							int loopcount = 0;
							tempClass = tempMap.get(it.next());
							if(tempClass.data < epoch)
							{
								while(loopcount < tempClass.repeatCount)
								{
									lessThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data > epoch)
							{
								while(loopcount < tempClass.repeatCount)
								{
									greaterThanAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
							else if(tempClass.data == epoch)
							{
								while(loopcount < tempClass.repeatCount)
								{
									equalAddressList.add(tempClass.address.get(loopcount));
									loopcount++;
								}
							}
						}
					}
					break;
				}
			}
			System.out.println();
			//Use arraylists of values < , > , = given value and print the rows column by column
			if(operator.equals("<"))
			{
				for(int print = 0; print < lessThanAddressList.size(); print++)
				{
					printResults(lessThanAddressList.get(print), tableName);
				}
			}
			else if(operator.equals("="))
			{
				for(int print = 0; print < equalAddressList.size(); print++)
				{
					printResults(equalAddressList.get(print), tableName);
				}
			}
			else if(operator.equals(">"))
			{
				for(int print = 0; print < greaterThanAddressList.size(); print++)
				{
					printResults(greaterThanAddressList.get(print), tableName);
				}
			}
			System.out.println();
		}		
		catch(Exception e)
		{
				e.printStackTrace();
		}
	}
	
	public static void printResults(long address, String tableName)
	{
		try
		{
			RandomAccessFile columnsTableFile = new RandomAccessFile("./Schemas/information_schema/Data/" + Properties.information_schemaColumnsTable, "rw");
			RandomAccessFile tempTableFile = new RandomAccessFile("./Schemas/"+Schema.selectedSchema+"/Data/"+Schema.selectedSchema+"."+tableName+".tbl", "rw");
			//Get column types for given table
			VarcharIndexClass tableNameClass = Schema.infoschemaColumnsTableNameMap.get(tableName);
			tempTableFile.seek(address);	//Point tempTable file to the location provided for the row to be printed
			for(int i = 0; i < tableNameClass.address.size(); i++)		//Get data type one by one and read from table data file accordingly
			{
				//System.out.println("Index :" + i);
				String dataType = "";
				long schemaNameAddress = tableNameClass.address.get(i);
				columnsTableFile.seek(schemaNameAddress);
				long tableNameAddress = schemaNameAddress + columnsTableFile.readByte() + 1;
				columnsTableFile.seek(tableNameAddress);
				long columnNameAddress = tableNameAddress + columnsTableFile.readByte() + 1;
				columnsTableFile.seek(columnNameAddress);
				long columnOrdinalPositionAddress = columnNameAddress + columnsTableFile.readByte() + 1; 
				columnsTableFile.seek(columnOrdinalPositionAddress);
				/*if(i == 0)
				{
					String columnName = "";
					//Find column Name
					int columnNameLength = columnsTableFile.readByte();
					for(int k = 0; k < columnNameLength; k++)
					{
						columnName = columnName + (char)columnsTableFile.readByte();
					}
					//Search for index file in the list of index files
					for(int x = 0; x < files.length; x++)
					{
						if(files[x].getPath().endsWith(columnName.toLowerCase()+".ndx"))
						{
							actualIndexFile = new RandomAccessFile(files[x].getPath(),"rw");	//assign object the correct file pointer that is the related index file
							break;
						}
					}
				}*/
				
				columnsTableFile.readInt();
				//columnTypeAddress = columnsTableFile.getFilePointer();
				int dataTypeLength = columnsTableFile.readByte();
				//System.out.println("dataType length :" + dataTypeLength);
				for(int j = 0; j < dataTypeLength; j++)
				{
					dataType = dataType + (char)columnsTableFile.readByte();
				}
				//System.out.println("before dataType :" + dataType);
				//Find out actual datatype name and size associated with it (if any)
				String regex = "(\\w+)(\\d+)?";
				Pattern pattern = Pattern.compile(regex);
				Matcher match = pattern.matcher(dataType);
				int dataSize = 0;
				int count = 0;
				while(match.find())
				{
					if(count == 0)
					dataType = match.group(0);
					else
					dataSize = Integer.parseInt(match.group(0));	
					count++;
				}
				//System.out.println("dataType :" + dataType);
				//System.out.println("dataSize :" + dataSize);
				
				
				//Read and print data from data table according to data type of the column
				
				if(dataType.equals("int"))	//If datatype found is INT type
				{
					System.out.print(tempTableFile.readInt() + " | ");		//read INT type data
				}
				else if(dataType.equals("byte"))	//If datatype found is BYTE type
				{
					System.out.print(tempTableFile.readByte() + " | ");		//read Byte type data
				}
				else if(dataType.equals("short"))	//If datatype found is SHORT type
				{
					System.out.print(tempTableFile.readShort() + " | ");		//read Short type data
				}	
				else if(dataType.equals("long"))	//If datatype found is LONG type
				{
					System.out.print(tempTableFile.readLong() + " | ");		//read Long type data
				}
				else if(dataType.equals("char"))	//If datatype found is CHAR type
				{
					int dataLength = tempTableFile.readByte();
					int readCount = 0;
					while(readCount < (dataSize-1))
					{
						char c = (char)tempTableFile.readByte();
						if(readCount < dataLength)
							System.out.print(c);
						
						readCount++;
					}
					/*while(readCount < dataSize)
					{
						tempTableFile.readByte();
						readCount++;
					}*/
					System.out.print(" | ");
				}
				else if(dataType.equals("varchar"))	//If datatype found is VARCHAR type
				{
					int dataLength = tempTableFile.readByte();
					int readCount = 0;
					while(readCount < dataLength)
					{
						System.out.print((char)tempTableFile.readByte());
						readCount++;
					}
					System.out.print(" | ");
				}
				else if(dataType.equals("float"))	//If datatype found is FLOAT type
				{
					System.out.print(tempTableFile.readFloat() + " | ");		//read Float type data
				}
				else if(dataType.equals("double"))	//If datatype found is DOUBLE type
				{
					System.out.print(tempTableFile.readDouble() + " | ");		//read Double type data
				}
				else if(dataType.equals("date"))	//If datatype found is DATE type
				{
					long epoch = tempTableFile.readLong();		//This contains long epoch data coverted from date string
				    Date date = new Date(epoch);
				    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				    String dateOutPut = sdf.format(date);
				    System.out.print(dateOutPut + " | ");
				}
				else if(dataType.equals("datetime"))	//If datatype found is DATETIME type
				{
					long epoch = tempTableFile.readLong();		//This contains long epoch data coverted from datetime string
				    Date date = new Date(epoch);
				    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
				    String dateTimeOutPut = sdf.format(date);
				    System.out.print(dateTimeOutPut + " | ");
				}
			}
			System.out.println();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
}


