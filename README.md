# 🗃️ Java Mini DBMS with Bitmap Indexing

This project implements a **miniature database engine** in Java that supports basic table creation, insertion, deletion, update, and querying. It also includes **bitmap indexing**, page-based storage, BRIN-style optimization, and custom serialization.

---

## 🚀 Features

- 📋 **Table Management**: Create tables with typed columns and clustering keys
- 🧾 **Insert / Update / Delete**: Supports row-level operations with constraints
- 📁 **Page-based Storage**: Tables are stored as multiple serialized page files
- 🔎 **Query Support**: Query tables with multiple conditions and logical operators (`AND`, `OR`, `XOR`)
- 🧠 **Bitmap Indexing**: Efficient query acceleration using bitmap indexes per column
- 🧮 **BRIN-like Optimization**: Query pruning based on value ranges

---

## 📦 Project Structure

| File/Folder                | Description |
|---------------------------|-------------|
| `DBApp.java`              | Main engine logic — handles all core operations |
| `metadata.csv` (in `/Data`) | Stores table schema and indexing metadata |
| `/Data/`                  | Stores serialized table pages and bitmap index pages |
| `/config/DBApp.properties`| Contains settings like page size, bitmap size, etc. |
| `Entry.java`              | Encodes bitmap index entries (not shown but referenced) |
| `SQLTerm.java`            | Represents a single SQL condition for querying |

---

## 🧠 Key Concepts

### Tables
- Each table consists of column definitions stored in `metadata.csv`
- Rows are stored across multiple **page files** (e.g., `Student1`, `Student2`, ...)

### Insertion
- Maintains sort order based on primary key
- Splits pages once a configurable max row count is reached
- Automatically updates bitmap indexes if available

### Bitmap Indexes
- Created per column using `createBitmapIndex(table, column)`
- Stored in folders like `/Data/StudentIndexOnGPA/`
- Run-length encoded for compression and performance

### Querying
- Queries are expressed using `SQLTerm[]` and logical operators
- Supports `=`, `!=`, `<`, `<=`, `>`, `>=` via bitmap evaluation or direct scanning
- Combines results using logical operations on bitmaps

---

## 📘 Example Usage

```java
DBApp db = new DBApp();
db.init();

Hashtable<String, String> schema = new Hashtable<>();
schema.put("id", "java.lang.Integer");
schema.put("name", "java.lang.String");
schema.put("gpa", "java.lang.Double");

db.createTable("Student", "id", schema);
db.createBitmapIndex("Student", "gpa");

Hashtable<String, Object> row = new Hashtable<>();
row.put("id", 1);
row.put("name", "Alice");
row.put("gpa", 3.8);
db.insertIntoTable("Student", row);
