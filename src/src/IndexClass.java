package src;
import java.util.ArrayList;
/*
 * These classes are used to store data into index files for all data types supported
 */
	public class IndexClass {
		Integer repeatCount = 0;
		ArrayList <Long> address = new  ArrayList <Long> ();
	}
	
	class LongIndexClass extends IndexClass{
		Long data;
	}
	
	class VarcharIndexClass extends IndexClass{
		String data;
	}
	
	class CharIndexClass extends IndexClass{
		String data;
	}
	
	class FloatIndexClass extends IndexClass{
		Float data;
	}
	
	class IntIndexClass extends IndexClass{
		Integer data;
	}
	
	class DoubleIndexClass extends IndexClass{
		Double data;
	}
	
	class ShortIndexClass extends IndexClass{
		Short data;
	}
	
	class ByteIndexClass extends IndexClass{
		Byte data;
	}
	
	class DateTimeIndexClass extends IndexClass{
		Long data;
	}
	
	class DateIndexClass extends IndexClass{
		Long data;
	}