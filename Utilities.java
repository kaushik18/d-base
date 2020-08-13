// Kaushik Nadimpalli
// Utilities.java - DavisBase Project
// Main Functionality of davisBase
// Contains methods for create, insert, delete, update, and drop
// Contains methods for datatypes and reading/storing user data
// Contains methods for B/B+ Tree functionality

// util libraries
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
// file (paging) libraries
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
// time released libraries
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class Utilities {
	public static int PAGESIZE = 512;
	public byte numberOfColumns; public byte[] dataType; public String[] data;
	public int pageNumber; public short payLoadSize; public byte pageType;
	public int rowId; public Utilities payload; public short location; public int pageNo;
	public Map<Integer, Utilities> records;
	public Utilities getPayload() {return payload;}
	public void setPayload(Utilities payload) {this.payload = payload;}
	public void setData(String[] data) {this.data = data;}
	public void setPageType(byte pageType) { this.pageType = pageType; }
	public void setPageNo(int pageNo) { this.pageNo = pageNo; }

	public static void createTable(RandomAccessFile table, String tableName, String[] columnNames) {
		try {
			table.setLength(PAGESIZE);
			table.seek(0);
			table.writeByte(0x0D);
			table.seek(2);
			table.writeShort(PAGESIZE);
			table.writeInt(-1);
			table.close();
			RandomAccessFile davisbaseTables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			int noOfPages = (int) (davisbaseTables.length() / PAGESIZE);
			int page = 0;
			Map<Integer, Utilities> recordUtilitiess = new LinkedHashMap<Integer, Utilities>();
			for (int i = 0; i < noOfPages; i++) {
				davisbaseTables.seek((i * PAGESIZE) + 4);
				int filePointer = davisbaseTables.readInt();
				if (filePointer == -1) {
					page = i;
					davisbaseTables.seek(i * PAGESIZE + 1);
					int noOfUtilitiess = davisbaseTables.readByte();
					short[] UtilitiesLocations = new short[noOfUtilitiess];
					davisbaseTables.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfUtilitiess; location++) {
						UtilitiesLocations[location] = davisbaseTables.readShort();
					}
					recordUtilitiess = getRecMetadata(davisbaseTables, UtilitiesLocations, i);
				}
			}
			davisbaseTables.close();
			Set<Integer> rowIds = recordUtilitiess.keySet();
			Set<Integer> sortedRowIds = new TreeSet<Integer>(rowIds);
			Integer rows[] = sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
			int key = rows[rows.length - 1] + 1;
			String[] values = { String.valueOf(key), tableName.trim(), "8", "10" };
			insertIntoTable("davisbase_tables", values);
			RandomAccessFile davisbaseColumns = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			noOfPages = (int) (davisbaseColumns.length() / PAGESIZE);
			page = 0;
			recordUtilitiess = new LinkedHashMap<Integer, Utilities>();
			for (int i = 0; i < noOfPages; i++) {
				davisbaseColumns.seek((i * PAGESIZE) + 4);
				int filePointer = davisbaseColumns.readInt();
				if (filePointer == -1) {
					page = i;
					davisbaseColumns.seek(i * PAGESIZE + 1);
					int noOfUtilitiess = davisbaseColumns.readByte();
					short[] UtilitiesLocations = new short[noOfUtilitiess];
					davisbaseColumns.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfUtilitiess; location++) {
						UtilitiesLocations[location] = davisbaseColumns.readShort();
					}
					recordUtilitiess = getRecMetadata(davisbaseColumns, UtilitiesLocations, i);
				}
			}
			rowIds = recordUtilitiess.keySet();
			sortedRowIds = new TreeSet<Integer>(rowIds);
			rows = sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
			key = rows[rows.length - 1];
			for (int i = 0; i < columnNames.length; i++) {
				key = key + 1;
				String[] coltemp = columnNames[i].split(" ");
				String isNullable = "YES";
				if (coltemp.length == 4) {
					if (coltemp[2].equalsIgnoreCase("NOT") && coltemp[3].equalsIgnoreCase("NULL")) {
						isNullable = "NO";
					}
					if (coltemp[2].equalsIgnoreCase("PRIMARY") && coltemp[3].equalsIgnoreCase("KEY")) {
						isNullable = "NO";
					}
				}
				String colName = coltemp[0];
				String dataType = coltemp[1].toUpperCase();
				String ordinalPosition = String.valueOf(i + 1);
				String[] val = { String.valueOf(key), tableName, colName, dataType, ordinalPosition, isNullable };
				insertIntoTable("davisbase_columns", val);
			}
		} catch (Exception e) { e.printStackTrace();	}
	}

	public static void dropTable(String tableName) {
		try {
			RandomAccessFile davisbaseTables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			updateMetadata(davisbaseTables, "davisbase_tables", tableName);
			RandomAccessFile davisbaseColumns = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			updateMetadata(davisbaseColumns, "davisbase_columns", tableName);
			File file = new File("data/userdata/" + tableName + ".tbl");
			if(file.delete()) {
			}else {
			FileOutputStream fp=new FileOutputStream(file);
			fp=null;
			}
		} catch (Exception e) { e.printStackTrace();	}
	}

	public static void insertIntoTable(String tableName, String[] values) {
		try {
			tableName = tableName.trim();
			String path = "data/userdata/" + tableName + ".tbl";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path = "data/catalog/" + tableName + ".tbl";
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			String condition[] = { "table_name", "=", tableName };
			String columnNames[] = { "*" };
			Map<Integer, Utilities> column = obtainRecord(tableName, columnNames, condition);
			String[] dataType = getDataType(column);
			int count = 0;
			String[] nullable = new String[column.size()];
			for (Map.Entry<Integer, Utilities> entry : column.entrySet()) {
				Utilities Utilities = entry.getValue();
				Utilities payload = Utilities.getPayload();
				String[] data = payload.data;
				nullable[count] = data[4];
				count++;
			}
			String[] isNullable = nullable;
			for (int i = 0; i < values.length; i++) {
				if (values[i].equalsIgnoreCase("null") && isNullable[i].equals("NO")) {
					System.out.println("Cannot insert NULL values in NOT NULL field");
					table.close();
					return;
				}
			}
			condition = new String[0];
			int pageNo = getPage(tableName, Integer.parseInt(values[0]));
			Map<Integer, Utilities> data = getData(tableName, columnNames, condition);
			if (data.containsKey(Integer.parseInt(values[0]))) {
				System.out.println("Duplicate value for primary key");
				System.out.println("Please enter unique record.");
				table.close();
				return;
			}
			byte[] plDataType = new byte[dataType.length - 1];
			int payLoadSize = getPageSize(tableName, values, plDataType, dataType);
			payLoadSize = payLoadSize + 6;
			int address = checkOverFlow(table, pageNo, payLoadSize);
			if (address != -1) {
				Utilities Utilities = createUtilities(pageNo, Integer.parseInt(values[0]), (short) payLoadSize, plDataType,
						values);
				payload(table, Utilities, address);
			} else {
				splitLeafNode(table, pageNo);
				int pNo = getPage(tableName, Integer.parseInt(values[0]));
				int addr = checkOverFlow(table, pNo, payLoadSize);
				Utilities Utilities = createUtilities(pNo, Integer.parseInt(values[0]), (short) payLoadSize, plDataType,
						values);
				payload(table, Utilities, addr);
			}
			table.close();
		} catch (Exception e) { e.printStackTrace();	}
	}

	public static void deleteTable(String tableName, String[] cond) throws IOException {
		String path = "data/userdata/" + tableName + ".tbl";
		if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
			path = "data/catalog/" + tableName + ".tbl";
		try {
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			String condition[] = { "table_name", "=", tableName };
			String columnNames[] = { "*" };
			Map<Integer, Utilities> column = obtainRecord(tableName, columnNames, condition);
			String[] dataType = getDataType(column);
			int count = 0;
			String[] nullable = new String[column.size()];
			for (Map.Entry<Integer, Utilities> entry : column.entrySet()) {
				Utilities Utilities = entry.getValue();
				Utilities payload = Utilities.getPayload();
				String[] data = payload.data;
				nullable[count] = data[4];
				count++;
			}
			String[] isNullable = nullable;
			Map<Integer, String> colNames = getRecordNames(tableName);
			condition = new String[0];
			int pageNo = getPage(tableName, Integer.parseInt(cond[2]));
			Map<Integer, Utilities> data = getData(tableName, columnNames, condition);
			if (data.containsKey(Integer.parseInt(cond[2]))) {
				table.seek((PAGESIZE * pageNo) + 1);
				int noOfUtilitiess = table.readByte();
				short[] UtilitiesLocations = new short[noOfUtilitiess];
				table.seek((PAGESIZE * pageNo) + 8);
				for (int location = 0; location < noOfUtilitiess; location++) {
					UtilitiesLocations[location] = table.readShort();
				}
				Map<Integer, Utilities> recordUtilitiess = new LinkedHashMap<Integer, Utilities>();
				recordUtilitiess = getRecMetadata(table, UtilitiesLocations, pageNo);
				String[] condition1 = { cond[0], "<>", cond[2] };
				String[] columnNames1 = { "*" };
				Map<Integer, Utilities> filteredRecs = filterRecordsByData(colNames, recordUtilitiess, columnNames, condition1);
				short[] offsets = new short[filteredRecs.size()];
				int l = 0;
				for (Map.Entry<Integer, Utilities> entry : filteredRecs.entrySet()) {
					Utilities Utilities = entry.getValue();
					offsets[l] = Utilities.location;
					table.seek(pageNo * PAGESIZE + 8 + (2 * l));
					table.writeShort(offsets[l]);
					l++;
				}
				table.seek((PAGESIZE * pageNo) + 1);
				table.writeByte(offsets.length);
				table.writeShort(offsets[offsets.length - 1]);
				table.close();
			}
		} catch (FileNotFoundException e) { e.printStackTrace();	}
	}

	public static void updateTable(String tableName, String[] set, String[] cond) {
		String path = "data/userdata/" + tableName + ".tbl";
		if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
			path = "data/catalog/" + tableName + ".tbl";
		try {
			RandomAccessFile file = new RandomAccessFile(path, "rw");
			String condition[] = { "table_name", "=", tableName };
			String columnNames[] = { "*" };
			Map<Integer, Utilities> column = obtainRecord(tableName, columnNames, condition);
			String[] dataType = getDataType(column);
			int count = 0;
			String[] nullable = new String[column.size()];
			for (Map.Entry<Integer, Utilities> entry : column.entrySet()) {
				Utilities Utilities = entry.getValue();
				Utilities payload = Utilities.getPayload();
				String[] data = payload.data;
				nullable[count] = data[4];
				count++;
			}
			String[] isNullable = nullable;
			Map<Integer, String> colNames = getRecordNames(tableName);
			int k = -1;
			for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
				String columnName = entry.getValue();
				if (columnName.equals(set[0])) {
					k = entry.getKey();
				}
			}
			if (cond.length > 0) {
				int key = Integer.parseInt(cond[2]);
				condition = new String[0];
				int pageno = getPage(tableName, Integer.parseInt(cond[2]));
				Map<Integer, Utilities> data = getData(tableName, columnNames, condition);
				if (data.containsKey(Integer.parseInt(cond[2]))) {
					try {
						file.seek((pageno) * PAGESIZE + 1);
						int records = file.read();
						short[] offsetLocations = new short[records];
						for (int j = 0; j < records; j++) {
							file.seek((pageno) * PAGESIZE + 8 + 2 * j);
							offsetLocations[j] = file.readShort();
							file.seek(offsetLocations[j] + 2);
							int ky = file.readInt();
							if (key == ky) {
								int no = file.read();
								byte[] sc = new byte[no];
								file.read(sc);
								int seek_positions = 0;
								for (int i = 0; i < k - 2; i++) {
									seek_positions += dataSize(sc[i]);
								}
								file.seek(offsetLocations[j] + 6 + no + 1 + seek_positions);
								byte sc_update = sc[k - 2];
								switch (sc_update) {
								case 0x00:
									file.write(Integer.parseInt(set[2]));
									sc[k - 2] = 0x04;
									break;
								case 0x01:
									file.writeShort(Integer.parseInt(set[2]));
									sc[k - 2] = 0x05;
									break;
								case 0x02:
									file.writeInt(Integer.parseInt(set[2]));
									sc[k - 2] = 0x06;
									break;
								case 0x03:
									file.writeDouble(Double.parseDouble(set[2]));
									sc[k - 2] = 0x09;
									break;
								case 0x04:
									file.write(Integer.parseInt(set[2]));
									break;
								case 0x05:
									file.writeShort(Integer.parseInt(set[2]));
									break;
								case 0x06:
									file.writeInt(Integer.parseInt(set[2]));
									break;
								case 0x07:
									file.writeLong(Long.parseLong(set[2]));
									break;
								case 0x08:
									file.writeFloat(Float.parseFloat(set[2]));
									break;
								case 0x09:
									file.writeDouble(Double.parseDouble(set[2]));
									break;
								}
								file.seek(offsetLocations[j] + 7);
								file.write(sc);
								file.close();
							}
						}
					} catch (Exception e) { e.printStackTrace();	}
				}
			} else {
				try {
					int no_of_pages = (int) (file.length() / PAGESIZE);
					for (int l = 0; l < no_of_pages; l++) {
						file.seek(l * PAGESIZE);
						byte pageType = file.readByte();
						if (pageType == 0x0D) {
							file.seek((l) * PAGESIZE + 1);
							int records = file.read();
							short[] offsetLocations = new short[records];
							for (int j = 0; j < records; j++) {
								file.seek((l) * PAGESIZE + 8 + 2 * j);
								offsetLocations[j] = file.readShort();
								file.seek(offsetLocations[j] + 6);
								int no = file.read();
								byte[] sc = new byte[no];
								file.read(sc);
								int seek_positions = 0;
								for (int i = 0; i < k - 2; i++) {
									seek_positions += dataSize(sc[i]);
								}
								file.seek(offsetLocations[j] + 6 + no + 1 + seek_positions);
								byte sc_update = sc[k - 2];
								switch (sc_update) {
								case 0x00:
									file.write(Integer.parseInt(set[2]));
									sc[k - 2] = 0x04;
									break;
								case 0x01:
									file.writeShort(Integer.parseInt(set[2]));
									sc[k - 2] = 0x05;
									break;
								case 0x02:
									file.writeInt(Integer.parseInt(set[2]));
									sc[k - 2] = 0x06;
									break;
								case 0x03:
									file.writeDouble(Double.parseDouble(set[2]));
									sc[k - 2] = 0x09;
									break;
								case 0x04:
									file.write(Integer.parseInt(set[2]));
									break;
								case 0x05:
									file.writeShort(Integer.parseInt(set[2]));
									break;
								case 0x06:
									file.writeInt(Integer.parseInt(set[2]));
									break;
								case 0x07:
									file.writeLong(Long.parseLong(set[2]));
									break;
								case 0x08:
									file.writeFloat(Float.parseFloat(set[2]));
									break;
								case 0x09:
									file.writeDouble(Double.parseDouble(set[2]));
									break;
								}
								file.seek(offsetLocations[j] + 7);
								file.write(sc);
							}}}
				} catch (Exception e) { e.printStackTrace();	}
			}
		} catch (Exception e) { e.printStackTrace();	}
	}

	private static void setRoot(RandomAccessFile table, int parent, int childPage, int midkey) {
		try {
			table.seek((parent - 1) * PAGESIZE + 1);
			int numrecords = table.read();
			if (checkNode(table, parent)) {
				int content = (parent) * PAGESIZE;
				TreeMap<Integer, Short> offsets = new TreeMap<Integer, Short>();
				if (numrecords == 0) {
					table.seek((parent - 1) * PAGESIZE + 1);
					table.write(1);
					content = content - 8;
					table.writeShort(content);
					table.writeInt(-1);
					table.writeShort(content);
					table.seek(content);
					table.writeInt(childPage + 1);
					table.writeInt(midkey);
				} else {
					table.seek((parent - 1) * PAGESIZE + 2);
					short UtilitiesContentArea = table.readShort();
					UtilitiesContentArea = (short) (UtilitiesContentArea - 8);
					table.seek(UtilitiesContentArea);
					table.writeInt(childPage + 1);
					table.writeInt(midkey);
					table.seek((parent - 1) * PAGESIZE + 2);
					table.writeShort(UtilitiesContentArea);
					for (int i = 0; i < numrecords; i++) {
						table.seek((parent - 1) * PAGESIZE + 8 + 2 * i);
						short off = table.readShort();
						table.seek(off + 4);
						int key = table.readInt();
						offsets.put(key, off);
					}
					offsets.put(midkey, UtilitiesContentArea);
					table.seek((parent - 1) * PAGESIZE + 1);
					table.write(numrecords++);
					table.seek((parent - 1) * PAGESIZE + 8);
					for (Entry<Integer, Short> entry : offsets.entrySet()) {
						table.writeShort(entry.getValue());
					}
				}
			} else {
				splitNode(table, parent);
			}
		} catch (IOException e) { e.printStackTrace();	}
	}

	private static void writePage(RandomAccessFile table, int parent, int newPage, int midKey) {
		try {
			table.seek((parent - 1) * PAGESIZE + 1);
			int numrecords = table.read();
			int mid = (int) Math.ceil((double) numrecords / 2);
			int numrecords1 = mid - 1;
			int numrecords2 = numrecords - numrecords1;
			int size = PAGESIZE;
			for (int i = numrecords1; i < numrecords; i++) {
				table.seek((parent - 1) * PAGESIZE + 8 + 2 * i);
				short offset = table.readShort();
				table.seek(offset);
				byte[] data = new byte[8];
				table.read(data);
				size = size - 8;
				table.seek((newPage - 1) * PAGESIZE + size);
				table.write(data);
				table.seek((newPage - 1) * PAGESIZE + 8 + (i - numrecords1) * 2);
				table.writeShort(size);
			}
			table.seek((parent - 1) * PAGESIZE + 1);
			table.write(numrecords1);
			table.seek((newPage - 1) * PAGESIZE + 1);
			table.write(numrecords2);
			int int_parent = getRoot(table, parent);
			if (int_parent == 0) {
				int newParent = createPage(table);
				setRoot(table, newParent, parent, midKey);
				table.seek((newParent - 1) * PAGESIZE + 4);
				table.writeInt(newPage);
			} else {
				if (pagePtr(table, int_parent, parent)) {
					setRoot(table, int_parent, parent, midKey);
					table.seek((int_parent - 1) * PAGESIZE + 4);
					table.writeInt(newPage);
				} else
					setRoot(table, int_parent, newPage, midKey);
			}
		} catch (IOException e) { e.printStackTrace();	}
	}

	private static boolean pagePtr(RandomAccessFile table, int parent, int pagePtr) {
		try {
			table.seek((parent - 1) * PAGESIZE + 4);
			if (table.readInt() == pagePtr)
				return true;
		} catch (IOException e) { e.printStackTrace();	}
		return false;
	}

	private static boolean checkNode(RandomAccessFile table, int parent) {
		try {
			table.seek((parent - 1) * PAGESIZE + 1);
			int numrecords = table.read();
			short Utilitiescontent = table.readShort();
			int size = 8 + numrecords * 2 + Utilitiescontent;
			size = PAGESIZE - size;
			if (size >= 8)
				return true;
		} catch (IOException e) { e.printStackTrace();	}
		return false;
	}

	private static int createPage(RandomAccessFile table) {
		int numpages = 0;
		try {
			numpages = (int) (table.length() / PAGESIZE);
			numpages++;
			table.setLength(table.length() + PAGESIZE);
			table.seek((numpages - 1) * PAGESIZE);
			table.write(0x05);
		} catch (IOException e) { e.printStackTrace();	}
		return numpages;
	}

	private static int getRoot(RandomAccessFile table, int page) {
		try {
			int numpages = (int) (table.length() / PAGESIZE);
			for (int i = 0; i < numpages; i++) {
				table.seek(i * PAGESIZE);
				byte pageType = table.readByte();
				if (pageType == 0x05) {
					table.seek(i * PAGESIZE + 4);
					int p = table.readInt();
					if (page == p)
						return i + 1;
					table.seek(i * PAGESIZE + 1);
					int numrecords = table.read();
					short[] offsets = new short[numrecords];
					for (int j = 0; j < numrecords; j++) {
						table.seek(i * PAGESIZE + 8 + 2 * j);
						offsets[i] = table.readShort();
						table.seek(offsets[i]);
						if (page == table.readInt())
							return j + 1;
					}
				}
			}
		} catch (IOException e) { e.printStackTrace();	}
		return 0;
	}

	private static int splitRecords(RandomAccessFile table, int pageNo) {
		int midKey = 0;
		try {
			table.seek((pageNo) * PAGESIZE);
			byte pageType = table.readByte();
			short numUtilitiess = table.readByte();
			short mid = (short) Math.ceil(numUtilitiess / 2);
			table.seek(pageNo * PAGESIZE + 8 + (2 * (mid - 1)));
			short addr = table.readShort();
			table.seek(addr);
			if (pageType == 0x0D)
				table.seek(addr + 2);
			else
				table.seek(addr + 4);
			midKey = table.readInt();
		} catch (Exception e) { e.printStackTrace();	}
		return midKey;
	}

	private static int createNewPage(RandomAccessFile table) {
		try {
			int noOfPages = (int) table.length() / PAGESIZE;
			noOfPages = noOfPages + 1;
			table.setLength(noOfPages * PAGESIZE);
			table.seek((noOfPages - 1) * PAGESIZE);
			table.writeByte(0x0D);
			return noOfPages;
		} catch (Exception e) { e.printStackTrace();	}
		return -1;
	}

	private static void splitNode(RandomAccessFile table, int parent) {
		int newPage = createPage(table);
		int midKey = splitRecords(table, parent - 1);
		writePage(table, parent, newPage, midKey);
		try {
			table.seek((parent - 1) * PAGESIZE + 4);
			int rightpage = table.readInt();
			table.seek((newPage - 1) * PAGESIZE + 4);
			table.writeInt(rightpage);
			table.seek((parent - 1) * PAGESIZE + 4);
			table.writeInt(newPage);
		} catch (IOException e) { e.printStackTrace();	}
	}

	public static void payload(RandomAccessFile file, Utilities Utilities, int UtilitiesLocation) {
		try {
			file.seek(UtilitiesLocation);
			file.writeShort(Utilities.payLoadSize);
			file.writeInt(Utilities.rowId);
			Utilities payload = Utilities.getPayload();
			file.writeByte(payload.numberOfColumns);
			byte[] dataTypes = payload.dataType;
			file.write(dataTypes);
			String data[] = payload.data;
			for (int i = 0; i < dataTypes.length; i++) {
				switch (dataTypes[i]) {
				case 0x00:
					file.writeByte(0);
					break;
				case 0x01:
					file.writeShort(0);
					break;
				case 0x02:
					file.writeInt(0);
					break;
				case 0x03:
					file.writeLong(0);
					break;
				case 0x04:
					file.writeByte(new Byte(data[i + 1]));
					break;
				case 0x05:
					file.writeShort(new Short(data[i + 1]));
					break;
				case 0x06:
					file.writeInt(new Integer(data[i + 1]));
					break;
				case 0x07:
					file.writeLong(new Long(data[i + 1]));
					break;
				case 0x08:
					file.writeFloat(new Float(data[i + 1]));
					break;
				case 0x09:
					file.writeDouble(new Double(data[i + 1]));
					break;
				case 0x0A:
					long datetime = file.readLong();
					ZoneId zoneId = ZoneId.of("America/Chicago");
					Instant x = Instant.ofEpochSecond(datetime);
					ZonedDateTime zdt2 = ZonedDateTime.ofInstant(x, zoneId);
					zdt2.toLocalTime();
					break;
				case 0x0B:
					long date = file.readLong();
					ZoneId zoneId1 = ZoneId.of("America/Chicago");
					Instant x1 = Instant.ofEpochSecond(date);
					ZonedDateTime zdt3 = ZonedDateTime.ofInstant(x1, zoneId1);
					zdt3.toLocalTime();
					break;
				default:
					file.writeBytes(data[i + 1]);
					break;
				}
			}
			file.seek((PAGESIZE * Utilities.pageNumber) + 1);
			int noOfUtilitiess = file.readByte();
			file.seek((PAGESIZE * Utilities.pageNumber) + 1);
			file.writeByte((byte) (noOfUtilitiess + 1));
			Map<Integer, Short> updateMap = new TreeMap<Integer, Short>();
			short[] UtilitiesLocations = new short[noOfUtilitiess];
			int[] keys = new int[noOfUtilitiess];
			for (int location = 0; location < noOfUtilitiess; location++) {
				file.seek((PAGESIZE * Utilities.pageNumber) + 8 + (location * 2));
				UtilitiesLocations[location] = file.readShort();
				file.seek(UtilitiesLocations[location] + 2);
				keys[location] = file.readInt();
				updateMap.put(keys[location], UtilitiesLocations[location]);
			}
			updateMap.put(Utilities.rowId, (short) UtilitiesLocation);
			file.seek((PAGESIZE * Utilities.pageNumber) + 8);
			for (Map.Entry<Integer, Short> entry : updateMap.entrySet()) {
				short offset = entry.getValue();
				file.writeShort(offset);
			}
			file.seek((PAGESIZE * Utilities.pageNumber) + 2);
			file.writeShort(UtilitiesLocation);
			file.close();
		} catch (Exception e) { e.printStackTrace();	}
	}

	public static Utilities createUtilities(int pageNo, int primaryKey, short payLoadSize, byte[] dataType, String[] values) {
		Utilities Utilities = new Utilities();
		Utilities.pageNumber = pageNo;
		Utilities.rowId = primaryKey;
		Utilities.payLoadSize = payLoadSize;
		Utilities payload = new Utilities();
		payload.numberOfColumns = (Byte.parseByte(values.length - 1 + ""));
		payload.dataType = dataType;
		payload.setData(values);
		Utilities.setPayload(payload);
		return Utilities;
	}

	private static void updateValue(RandomAccessFile table, int currentPage, int newPage, int midKey) {
		try {
			table.seek((currentPage) * PAGESIZE);
			byte pageType = table.readByte();
			int noOfUtilitiess = table.readByte();
			int mid = (int) Math.ceil(noOfUtilitiess / 2);
			int lower = mid - 1;
			int upper = noOfUtilitiess - lower;
			int content = 512;
			for (int i = mid; i <= noOfUtilitiess; i++) {
				table.seek(currentPage * PAGESIZE + 8 + (2 * i) - 2);
				short offset = table.readShort();
				table.seek(offset);
				int UtilitiesSize = table.readShort() + 6;
				content = content - UtilitiesSize;
				table.seek(offset);
				byte[] Utilities = new byte[UtilitiesSize];
				table.read(Utilities);
				table.seek((newPage - 1) * PAGESIZE + content);
				table.write(Utilities);
				table.seek((newPage - 1) * PAGESIZE + 8 + (i - mid) * 2);
				table.writeShort((newPage - 1) * PAGESIZE + content);
			}

			table.seek((newPage - 1) * PAGESIZE + 2);
			table.writeShort((newPage - 1) * PAGESIZE + content);
			table.seek((currentPage) * PAGESIZE + 8 + (lower * 2));
			short offset = table.readShort();
			table.seek((currentPage) * PAGESIZE + 2);
			table.writeShort(offset);
			table.seek((currentPage) * PAGESIZE + 4);
			int pagePtr = table.readInt();
			table.seek((newPage - 1) * PAGESIZE + 4);
			table.writeInt(pagePtr);
			table.seek((currentPage) * PAGESIZE + 4);
			table.writeInt(newPage);
			byte Utilitiess = (byte) lower;
			table.seek((currentPage) * PAGESIZE + 1);
			table.writeByte(Utilitiess);
			Utilitiess = (byte) upper;
			table.seek((newPage - 1) * PAGESIZE + 1);
			table.writeByte(Utilitiess);
			int parent = getRoot(table, currentPage + 1);
			if (parent == 0) {
				int parentpage = createPage(table);
				setRoot(table, parentpage, currentPage, midKey);
				table.seek((parentpage - 1) * PAGESIZE + 4);
				table.writeInt(newPage);
			} else {
				if (pagePtr(table, parent, currentPage + 1)) {
					setRoot(table, parent, currentPage, midKey);
					table.seek((parent - 1) * PAGESIZE + 4);
					table.writeInt(newPage);
				} else {
					setRoot(table, parent, newPage, midKey);
				}
			}
		} catch (Exception e) {
			System.out.println("Error at splitLeafNodePage");
			e.printStackTrace();
		}
	}

	public static int checkOverFlow(RandomAccessFile file, int page, int payLoadsize) {
		int val = -1;
		try {
			file.seek((page) * PAGESIZE + 2);
			int content = file.readShort();
			if (content == 0)
				return PAGESIZE - payLoadsize;
			file.seek((page) * PAGESIZE + 1);
			int noOfUtilitiess = file.read();
			int pageHeaderSize = 8 + 2 * noOfUtilitiess + 2;
			file.seek((page) * PAGESIZE + 2);
			short startArea = (short) ((page + 1) * PAGESIZE - file.readShort());
			int space = startArea + pageHeaderSize;
			int spaceAvail = PAGESIZE - space;
			if (spaceAvail >= payLoadsize) {
				file.seek((page) * PAGESIZE + 2);
				short offset = file.readShort();
				return offset - payLoadsize;
			}
		} catch (Exception e) { e.printStackTrace();	}
		return val;
	}

	private static int getPageSize(String tableName, String[] values, byte[] plDataType, String[] dataType) {
		int size = 1 + dataType.length - 1;
		for (int i = 0; i < values.length - 1; i++) {
			plDataType[i] = dataType(values[i + 1], dataType[i + 1]);
			size = size + dataSize(plDataType[i]);
		} return size;
	}

	private static byte dataType(String value, String dataType) {
		if (value.equals("null")) {
			switch (dataType) {
			case "TINYINT":
				return 0x00;
			case "SMALLINT":
				return 0x01;
			case "INT":
				return 0x02;
			case "BIGINT":
				return 0x03;
			case "REAL":
				return 0x02;
			case "DOUBLE":
				return 0x03;
			case "FLOAT":
				return 0x03;
			case "DATETIME":
				return 0x03;
			case "DATE":
				return 0x03;
			case "TEXT":
				return 0x03;
			default:
				return 0x00;
			}
		} else {
			switch (dataType) {
			case "TINYINT":
				return 0x04;
			case "SMALLINT":
				return 0x05;
			case "INT":
				return 0x06;
			case "BIGINT":
				return 0x07;
			case "REAL":
				return 0x08;
			case "DOUBLE":
				return 0x09;
			case "FLOAT":
				return 0x09;
			case "DATETIME":
				return 0x0A;
			case "DATE":
				return 0x0B;
			case "TEXT":
				return (byte) (value.length() + 0x0C);
			default:
				return 0x00;
			}}
	}

	public static int getPage(String tableName, int key) {
		try {
			tableName = tableName.trim();
			String path = "data/userdata/" + tableName + ".tbl";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path = "data/catalog/" + tableName + ".tbl";
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);
			Map<Integer, String> colNames = getRecordNames(tableName);
			Map<Integer, Utilities> records = new LinkedHashMap<Integer, Utilities>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {
					int noOfUtilitiess = table.readByte();
					short[] UtilitiesLocations = new short[noOfUtilitiess];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfUtilitiess; location++) {
						UtilitiesLocations[location] = table.readShort();
					}
					Map<Integer, Utilities> recordUtilitiess = new LinkedHashMap<Integer, Utilities>();
					recordUtilitiess = getRecMetadata(table, UtilitiesLocations, i);
					Set<Integer> rowIds = recordUtilitiess.keySet();
					Set<Integer> sortedRowIds = new TreeSet<Integer>(rowIds);
					Integer rows[] = sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
					table.seek((PAGESIZE * i) + 4);
					int filePointer = table.readInt();
					if (rowIds.size() == 0) {
						table.close();
						return 0;}
					if (rows[0] <= key && key <= rows[rows.length - 1]) {
						table.close();
						return i;}
					else if (filePointer == -1 && rows[rows.length - 1] < key) {
						table.close();
						return i;
					}
				}
			}
		}
		catch (Exception e) { e.printStackTrace();	}
		return -1;
	}

	public static Map<Integer, Utilities> getData(String tableName, String[] columnNames, String[] condition) {
		try {
			tableName = tableName.trim();
			String path = "data/userdata/" + tableName + ".tbl";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path = "data/catalog/" + tableName + ".tbl";
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);
			Map<Integer, Utilities> pageInfo = new LinkedHashMap<Integer, Utilities>();
			Map<Integer, String> colNames = getRecordNames(tableName);
			Map<Integer, Utilities> records = new LinkedHashMap<Integer, Utilities>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {
					Utilities page = new Utilities();
					page.setPageNo(i);
					page.setPageType(pageType);
					int noOfUtilitiess = table.readByte();
					short[] UtilitiesLocations = new short[noOfUtilitiess];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfUtilitiess; location++) {
						UtilitiesLocations[location] = table.readShort();
					}
					Map<Integer, Utilities> recordUtilitiess = new LinkedHashMap<Integer, Utilities>();
					recordUtilitiess = getRecMetadata(table, UtilitiesLocations, i);
					page.records = recordUtilitiess;
					pageInfo.put(i, page);
					records.putAll(recordUtilitiess);
				}
			}
			if (condition.length > 0) {
				Map<Integer, Utilities> filteredRecords = filterRecords(colNames, records, columnNames, condition);
				table.close();
				return filteredRecords;
			} else {
				table.close();
				return records;
			}
		} catch (Exception e) { e.printStackTrace();	}
		return null;
	}

	public static String[] getDataType(Map<Integer, Utilities> column) {
		int count = 0;
		String[] dataType = new String[column.size()];
		for (Map.Entry<Integer, Utilities> entry : column.entrySet()) {
			Utilities Utilities = entry.getValue();
			Utilities payload = Utilities.getPayload();
			String[] data = payload.data;
			dataType[count] = data[2];
			count++;
		} return dataType;
	}

	public static Map<Integer, Utilities> obtainRecord(String tableName, String[] columnNames, String[] condition) {
		try {
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);
			Map<Integer, String> colNames = getRecordNames("davisbase_columns");
			Map<Integer, Utilities> records = new LinkedHashMap<Integer, Utilities>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {
					int noOfUtilitiess = table.readByte();
					short[] UtilitiesLocations = new short[noOfUtilitiess];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfUtilitiess; location++) {
						UtilitiesLocations[location] = table.readShort();
					}
					Map<Integer, Utilities> recordUtilitiess = new LinkedHashMap<Integer, Utilities>();
					recordUtilitiess = getRecMetadata(table, UtilitiesLocations, i);
					records.putAll(recordUtilitiess);
				}
			}
			if (condition.length > 0) {
				Map<Integer, Utilities> filteredRecords = filterRecords(colNames, records, columnNames, condition);
				table.close();
				return filteredRecords;
			} else {
				table.close();
				return records;
			}
		} catch (Exception e) { e.printStackTrace();	}
		return null;
	}

	public static void displayResults(Map<Integer, String> colNames, Map<Integer, Utilities> records) {
		String colString = "";
		String recString = "";
		ArrayList<String> recList = new ArrayList<String>();
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String colName = entry.getValue();
			colString += colName + " | ";
		}
		System.out.println(colString);
		for (Map.Entry<Integer, Utilities> entry : records.entrySet()) {
			Utilities Utilities = entry.getValue();
			recString += Utilities.rowId;
			String data[] = Utilities.getPayload().data;
			for (String dataS : data) {
				recString = recString + " | " + dataS;
			}
			System.out.println(recString);
			recString = "";
		}
	}

	public static void userQuery(String tableName, String[] columnNames, String[] condition) {
		try {
			tableName = tableName.trim();
			String path = "data/userdata/" + tableName + ".tbl";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path = "data/catalog/" + tableName + ".tbl";
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);
			Map<Integer, String> colNames = getRecordNames(tableName);
			Map<Integer, Utilities> records = new LinkedHashMap<Integer, Utilities>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {
					int noOfUtilitiess = table.readByte();
					short[] UtilitiesLocations = new short[noOfUtilitiess];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfUtilitiess; location++) {
						UtilitiesLocations[location] = table.readShort();
					}
					Map<Integer, Utilities> recordUtilitiess = new LinkedHashMap<Integer, Utilities>();
					recordUtilitiess = getRecMetadata(table, UtilitiesLocations, i);
					records.putAll(recordUtilitiess);
				}
			}
			if (condition.length > 0) {
				Map<Integer, Utilities> filteredRecords = filterRecords(colNames, records, columnNames, condition);
				displayResults(colNames, filteredRecords);
			} else {
				if (records.isEmpty()) {
					System.out.println("Empty Set..");
				} else {
					displayResults(colNames, records);
				}
			}
			table.close();
		} catch (Exception e) { e.printStackTrace();	}
	}

	private static Map<Integer, Utilities> filterRecords(Map<Integer, String> colNames, Map<Integer, Utilities> records, String[] resultColumnNames, String[] condition) {
		Set<String> resultColumnSet = new HashSet<String>(Arrays.asList(resultColumnNames));
		Map<Integer, Utilities> filteredRecords = new LinkedHashMap<Integer, Utilities>();
		int whereOrdinalPosition = 2;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				whereOrdinalPosition = entry.getKey();
			}
		}
		Set<Integer> ordinalPositions = colNames.keySet();
		for (Map.Entry<Integer, Utilities> entry : records.entrySet()) {
			Utilities Utilities = entry.getValue();
			Utilities payload = Utilities.getPayload();
			String[] data = payload.data;
			byte[] dataTypeCodes = payload.dataType;
			boolean result;
			if (whereOrdinalPosition == 1)
				result = checkInformation((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = checkInformation(dataTypeCodes[whereOrdinalPosition - 2], data[whereOrdinalPosition - 2], condition);
			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		} return filteredRecords;
	}

	private static Map<Integer, Utilities> filterRecordsByData(Map<Integer, String> colNames,
			Map<Integer, Utilities> records, String[] resultColumnNames, String[] condition) {
		Set<String> resultColumnSet = new HashSet<String>(Arrays.asList(resultColumnNames));
		Map<Integer, Utilities> filteredRecords = new LinkedHashMap<Integer, Utilities>();
		int whereOrdinalPosition = 2;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				whereOrdinalPosition = entry.getKey();
			}
		}
		Set<Integer> ordinalPositions = colNames.keySet();
		for (Map.Entry<Integer, Utilities> entry : records.entrySet()) {
			Utilities Utilities = entry.getValue();
			Utilities payload = Utilities.getPayload();
			String[] data = payload.data;
			byte[] dataTypeCodes = payload.dataType;
			boolean result;
			if (whereOrdinalPosition == 1)
				result = checkInformation((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = checkInformation(dataTypeCodes[whereOrdinalPosition - 2], data[whereOrdinalPosition - 2], condition);
			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		} return filteredRecords;
	}

	private static boolean checkInformation(byte code, String data, String[] condition) {
		if (code >= 0x04 && code <= 0x07) {
			Long dataLong = Long.parseLong(data);
			switch (condition[1]) {
			case "=":
				if (dataLong == Long.parseLong(condition[2]))
					return true;
				break;
			case ">":
				if (dataLong > Long.parseLong(condition[2]))
					return true;
				break;
			case "<":
				if (dataLong < Long.parseLong(condition[2]))
					return true;
				break;
			case "<=":
				if (dataLong <= Long.parseLong(condition[2]))
					return true;
				break;
			case ">=":
				if (dataLong >= Long.parseLong(condition[2]))
					return true;
				break;
			case "<>":
				if (dataLong != Long.parseLong(condition[2]))
					return true;
				break;
			default:
				System.out.println("Cannot recognize operator");
				return false;
			}
		} else if (code == 0x08 || code == 0x09) {
			Double doubleData = Double.parseDouble(data);
			switch (condition[1]) {
			case "=":
				if (doubleData == Double.parseDouble(condition[2]))
					return true;
				break;
			case ">":
				if (doubleData > Double.parseDouble(condition[2]))
					return true;
				break;
			case "<":
				if (doubleData < Double.parseDouble(condition[2]))
					return true;
				break;
			case "<=":
				if (doubleData <= Double.parseDouble(condition[2]))
					return true;
				break;
			case ">=":
				if (doubleData >= Double.parseDouble(condition[2]))
					return true;
				break;
			case "<>":
				if (doubleData != Double.parseDouble(condition[2]))
					return true;
				break;
			default:
				System.out.println("Cannot recognize operator");
				return false;
			}
		} else if (code >= 0x0C) {
			condition[2] = condition[2].replaceAll("'", "");
			condition[2] = condition[2].replaceAll("\"", "");
			switch (condition[1]) {
			case "=":
				if (data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			case "<>":
				if (!data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			default:
				System.out.println("Cannot recognize operator");
				return false;
			}}
		return false;
	}

	public static Map<Integer, String> getRecordNames(String tableName) {
		Map<Integer, String> columns = new LinkedHashMap<Integer, String>();
		try {
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {
					int noOfUtilitiess = table.readByte();
					short[] UtilitiesLocations = new short[noOfUtilitiess];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfUtilitiess; location++) {
						UtilitiesLocations[location] = table.readShort();
					}
					Map<Integer, Utilities> recordUtilitiess = new LinkedHashMap<Integer, Utilities>();
					recordUtilitiess = getRecMetadata(table, UtilitiesLocations, i);

					for (Map.Entry<Integer, Utilities> entry : recordUtilitiess.entrySet()) {

						Utilities Utilities = entry.getValue();

						Utilities payload = Utilities.getPayload();
						String[] data = payload.data;
						if (data[0].equalsIgnoreCase(tableName)) {
							columns.put(Integer.parseInt(data[3]), data[1]);
						}}}}
			table.close();
		} catch (Exception e) {e.printStackTrace(); }
		return columns;
	}

	private static Map<Integer, Utilities> getRecMetadata(RandomAccessFile table, short[] UtilitiesLocations, int pageNo) {
		Map<Integer, Utilities> Utilitiess = new LinkedHashMap<Integer, Utilities>();
		for (int position = 0; position < UtilitiesLocations.length; position++) {
			try {
				Utilities Utilities = new Utilities();
				Utilities.pageNumber = pageNo;
				Utilities.location = UtilitiesLocations[position];
				table.seek(UtilitiesLocations[position]);
				short payLoadSize = table.readShort();
				Utilities.payLoadSize = payLoadSize;
				int rowId = table.readInt();
				Utilities.rowId = rowId;
				Utilities payload = new Utilities();
				byte num_cols = table.readByte();
				payload.numberOfColumns = num_cols;
				byte[] dataType = new byte[num_cols];
				int colsRead = table.read(dataType);
				payload.dataType = dataType;
				String data[] = new String[num_cols];
				payload.setData(data);
				for (int i = 0; i < num_cols; i++) {
					switch (dataType[i]) {
					case 0x00:
						data[i] = Integer.toString(table.readByte());
						data[i] = "null";
						break;
					case 0x01:
						data[i] = Integer.toString(table.readShort());
						data[i] = "null";
						break;
					case 0x02:
						data[i] = Integer.toString(table.readInt());
						data[i] = "null";
						break;
					case 0x03:
						data[i] = Long.toString(table.readLong());
						data[i] = "null";
						break;
					case 0x04:
						data[i] = Integer.toString(table.readByte());
						break;
					case 0x05:
						data[i] = Integer.toString(table.readShort());
						break;
					case 0x06:
						data[i] = Integer.toString(table.readInt());
						break;
					case 0x07:
						data[i] = Long.toString(table.readLong());
						break;
					case 0x08:
						data[i] = String.valueOf(table.readFloat());
						break;
					case 0x09:
						data[i] = String.valueOf(table.readDouble());
						break;
					case 0x0A:
						long tmp = table.readLong();
						Date dateTime = new Date(tmp);
						break;
					case 0x0B:
						long tmp1 = table.readLong();
						Date date = new Date(tmp1);
						break;
					default:
						int len = new Integer(dataType[i] - 0x0C);
						byte[] bytes = new byte[len];
						for (int j = 0; j < len; j++)
							bytes[j] = table.readByte();
						data[i] = new String(bytes);
						break;
					} }
				Utilities.setPayload(payload);
				Utilitiess.put(rowId, Utilities);
			} catch (Exception e) { e.printStackTrace();	}
		}
		return Utilitiess;
	}

	private static void splitLeafNode(RandomAccessFile table, int currentPage) {
		int newPage = createNewPage(table);
		int midKey = splitRecords(table, currentPage);
		updateValue(table, currentPage, newPage, midKey);
	}

	private static short dataSize(byte codes) {
		switch (codes) {
		case 0x00:
			return 1;
		case 0x01:
			return 2;
		case 0x02:
			return 4;
		case 0x03:
			return 8;
		case 0x04:
			return 1;
		case 0x05:
			return 2;
		case 0x06:
			return 4;
		case 0x07:
			return 8;
		case 0x08:
			return 4;
		case 0x09:
			return 8;
		case 0x0A:
			return 8;
		case 0x0B:
			return 8;
		default:
			return (short) (codes - 0x0C);
		}
	}

	public static String[] queryParse(String str) {
		String condition[] = new String[3];
		String values[] = new String[2];
		if (str.contains("=")) {
			values = str.split("=");
			condition[0] = values[0].trim();
			condition[1] = "=";
			condition[2] = values[1].trim();
		}
		if (str.contains(">")) {
			values = str.split(">");
			condition[0] = values[0].trim();
			condition[1] = ">";
			condition[2] = values[1].trim();
		}
		if (str.contains("<")) {
			values = str.split("<");
			condition[0] = values[0].trim();
			condition[1] = "<";
			condition[2] = values[1].trim();
		}
		if (str.contains(">=")) {
			values = str.split(">=");
			condition[0] = values[0].trim();
			condition[1] = ">=";
			condition[2] = values[1].trim();
		}
		if (str.contains("<=")) {
			values = str.split("<=");
			condition[0] = values[0].trim();
			condition[1] = "<=";
			condition[2] = values[1].trim();
		}
		if (str.contains("<>")) {
			values = str.split("<>");
			condition[0] = values[0].trim();
			condition[1] = "<>";
			condition[2] = values[1].trim();
		} return condition;
	}

	public static void updateMetadata(RandomAccessFile davisbaseTables, String metaTable, String tableName) throws IOException {
		int noOfPages = (int) (davisbaseTables.length() / PAGESIZE);
		Map<Integer, String> colNames = getRecordNames(metaTable);
		for (int i = 0; i < noOfPages; i++) {
			davisbaseTables.seek(PAGESIZE * i);
			byte pageType = davisbaseTables.readByte();
			if (pageType == 0x0D) {
				int noOfUtilitiess = davisbaseTables.readByte();
				short[] UtilitiesLocations = new short[noOfUtilitiess];
				davisbaseTables.seek((PAGESIZE * i) + 8);
				for (int location = 0; location < noOfUtilitiess; location++) {
					UtilitiesLocations[location] = davisbaseTables.readShort();
				}
				Map<Integer, Utilities> recordUtilitiess = new LinkedHashMap<Integer, Utilities>();
				recordUtilitiess = getRecMetadata(davisbaseTables, UtilitiesLocations, i);
				String[] condition = { "table_name", "<>", tableName };
				String[] columnNames = { "*" };
				Map<Integer, Utilities> filteredRecs = filterRecordsByData(colNames, recordUtilitiess, columnNames, condition);
				short[] offsets = new short[filteredRecs.size()];
				int l = 0;
				for (Map.Entry<Integer, Utilities> entry : filteredRecs.entrySet()) {
					Utilities Utilities = entry.getValue();
					offsets[l] = Utilities.location;
					davisbaseTables.seek(i * PAGESIZE + 8 + (2 * l));
					davisbaseTables.writeShort(offsets[l]);
					l++;
				}
				davisbaseTables.seek((PAGESIZE * i) + 1);
				davisbaseTables.writeByte(offsets.length);
				davisbaseTables.writeShort(offsets[offsets.length - 1]);
			}}
	}

	public static void createDBTables() {
		try {
			@SuppressWarnings("resource")
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			table.setLength(PAGESIZE * 1);
			table.seek(0);
			table.write(0x0D);
			table.write(0x02);
			table.writeShort(PAGESIZE - 32 - 33);
			table.writeInt(-1);
			table.writeShort(PAGESIZE - 32);
			table.writeShort(PAGESIZE - 32 - 33);
			table.seek(PAGESIZE - 32);
			table.writeShort(26);
			table.writeInt(1);
			table.writeByte(3);
			table.writeByte(28);
			table.write(0x06);
			table.write(0x05);
			table.writeBytes("davisbase_tables");
			table.writeInt(2);
			table.writeShort(34);
			table.seek(PAGESIZE - 32 - 33);
			table.writeShort(19);
			table.writeInt(2);
			table.writeByte(3);
			table.writeByte(29);
			table.write(0x06);
			table.write(0x05);
			table.writeBytes("davisbase_columns");
			table.writeInt(10);
			table.writeShort(34);
		} catch (Exception e) { e.printStackTrace();	}
	}
	public static void createDBColumns() {
		int UtilitiesHeader = 6;
		try {
			@SuppressWarnings("resource")
			RandomAccessFile column = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			column.setLength(PAGESIZE * 1);
			column.seek(0);
			column.write(0x0D);
			column.write(10);
			int recordSize[] = new int[] { 33, 39, 40, 43, 34, 40, 41, 39, 49, 41 };
			int offset[] = new int[10];
			offset[0] = PAGESIZE - recordSize[0] - UtilitiesHeader;
			column.seek(4);
			column.writeInt(-1);
			for (int i = 1; i < offset.length; i++) {
				offset[i] = offset[i - 1] - (recordSize[i] + UtilitiesHeader);
			}
			column.seek(2);
			column.writeShort(offset[9]);
			column.seek(8);
			for (int i = 0; i < offset.length; i++) {
				column.writeShort(offset[i]);
			}
			column.seek(offset[0]);
			column.writeShort(recordSize[0]);
			column.writeInt(1);
			column.writeByte(5);
			column.write(28);
			column.write(17);
			column.write(15);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_tables");
			column.writeBytes("rowid");
			column.writeBytes("INT");
			column.write(1);
			column.writeBytes("NO");
			column.seek(offset[1]);
			column.writeShort(recordSize[1]);
			column.writeInt(2);
			column.writeByte(5);
			column.write(28);
			column.write(22);
			column.write(16);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_tables");
			column.writeBytes("table_name");
			column.writeBytes("TEXT");
			column.write(2);
			column.writeBytes("NO");
			column.seek(offset[2]);
			column.writeShort(recordSize[2]);
			column.writeInt(3);
			column.writeByte(5);
			column.write(28);
			column.write(24);
			column.write(15);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_tables");
			column.writeBytes("record_count");
			column.writeBytes("INT");
			column.write(3);
			column.writeBytes("NO");
			column.seek(offset[3]);
			column.writeShort(recordSize[3]);
			column.writeInt(4);
			column.writeByte(5);
			column.write(28);
			column.write(22);
			column.write(20);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_tables");
			column.writeBytes("avg_length");
			column.writeBytes("SMALLINT");
			column.write(4);
			column.writeBytes("NO");
			column.seek(offset[4]);
			column.writeShort(recordSize[4]);
			column.writeInt(5);
			column.writeByte(5);
			column.write(29);
			column.write(17);
			column.write(15);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("rowid");
			column.writeBytes("INT");
			column.write(1);
			column.writeBytes("NO");
			column.seek(offset[5]);
			column.writeShort(recordSize[5]);
			column.writeInt(6);
			column.writeByte(5);
			column.write(29);
			column.write(22);
			column.write(16);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("table_name");
			column.writeBytes("TEXT");
			column.write(2);
			column.writeBytes("NO");
			column.seek(offset[6]);
			column.writeShort(recordSize[6]);
			column.writeInt(7);
			column.writeByte(5);
			column.write(29);
			column.write(23);
			column.write(16);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("column_name");
			column.writeBytes("TEXT");
			column.write(3);
			column.writeBytes("NO");
			column.seek(offset[7]);
			column.writeShort(recordSize[7]);
			column.writeInt(8);
			column.writeByte(5);
			column.write(29);
			column.write(21);
			column.write(16);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("data_type");
			column.writeBytes("TEXT");
			column.write(4);
			column.writeBytes("NO");
			column.seek(offset[8]);
			column.writeShort(recordSize[8]);
			column.writeInt(9);
			column.writeByte(5);
			column.write(29);
			column.write(28);
			column.write(19);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("ordinal_position");
			column.writeBytes("TINYINT");
			column.write(5);
			column.writeBytes("NO");
			column.seek(offset[9]);
			column.writeShort(recordSize[9]);
			column.writeInt(10);
			column.writeByte(5);
			column.write(29);
			column.write(23);
			column.write(16);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("is_nullable");
			column.writeBytes("TEXT");
			column.write(6);
			column.writeBytes("NO");
		} catch (Exception e) { e.printStackTrace();	}
	}

	public static void defineDB() {
		File data = new File("data/catalog");
		File userData = new File("data/userdata");
		data.mkdir(); userData.mkdir();
		createDBTables();
		createDBColumns(); }
}
