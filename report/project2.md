## Project 2 Report

### 1. Basic information

- Team #:
- Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-winter22-linbeier.git
- Student 1 UCI NetID:xiaobl3
- Student 1 Name:Xiaobin Lin
- Student 2 UCI NetID (if applicable):
- Student 2 Name (if applicable):

### 2. Meta-data

- Show your meta-data design (Tables and Columns table) and information about each column.
- Tables Table is named as "
  Tables", attribute table is named as "Columns".  
  Tables Table has 3 attributes: "table-id", "table-name", "file-name", their types are the same as P2
  DescriptorTables (table-id:int, table-name:varchar(50), file-name:varchar(50)).  
  Attribute Table has 5 attributes: table-id:int, column-name:varchar(50), column-type:int, column-length:int,
  column-position:int

### 3. Internal Record Format (in case you have changed from P1, please re-enter here)

- Show your record format design.  
  the same as p1

- Describe how you store a null field.  
  the same as p1

- Describe how you store a VarChar field.  
  the same as p1

- Describe how your record design satisfies O(1) field access.  
  the same as p1

### 4. Page Format (in case you have changed from P1, please re-enter here)

- Show your page format design.  
  the same as p1

- Explain your slot directory design if applicable.  
  the same as p1

### 5. Page Management (in case you have changed from P1, please re-enter here)

- How many hidden pages are utilized in your design?  
  I used 1 + (sizeof(int) * total page number) / PAGE_SIZE hidden pages

- Show your hidden page(s) format design if applicable 1 hidden page for recording read/write/append counter and also
  the total page number.  
  Other hidden pages are used to store free space of each page, every free space take 2 bytes in hidden page, and they
  are compact stored. And they are stored in file named main_file_name + "_aux"

### 6. Describe the following operation logic.

- Delete a record

1. check if record has been deleted.
2. check if record is a tombstone. if yes, recursive delete every record tombstone pointed to
3. every record after current record shift left and update slot information

- Update a record

1. access the real record(not tombstones)
2. if updated record shorter than current record, shrink current record, and shift left all records followed
3. if updated record longer than current record, check if current page's free space is enough for it. If yes, write it
   and shift right all records followed
4. if no enough free space for it, insert it in other page that has enough free space and write back a tombstone in
   original record's place. Note that records update in other page will be mark as internal record.


- Scan on normal records  
  initialize information in rm_scanIterator, whenever users call getNextRecord, it will find next record that satisfy
  with condition

- Scan on deleted records  
  will dismiss deleted records

- Scan on updated records  
  will find the real record(not tombstones). Also scan will not read internal records

### 7. Implementation Detail

- Other implementation details goes here.

1. User control: Not allowed user to modify(including create/delete table, insert/update/delete tuples) table named "
   Tables" or "Columns" by checking tableName. If table name is "
   Tables" or "Columns", function will return error.
2. I used first bit of record length as flag indicates whether this record is internal.
3. Deleted record will change its length to PAGE_SIZE + 1
4. Since I don't change the position of deleted record's slot, the slot will no longer keep the same sequence as
   records, so I introduced a transform layer for scan operation

### 8. Member contribution (for team of two)

- Explain how you distribute the workload in team.

### 9. Other (optional)

- Freely use this section to tell us about things that are related to the project 1, but not related to the other
  sections (optional)  
  It will be great if you clarify the memory limit and data scope, they will help me in designing system.

- Feedback on the project to help improve the project. (optional)