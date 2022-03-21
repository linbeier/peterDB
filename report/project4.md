## Project 4 Report

### 1. Basic information

- Team #:
- Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-winter22-linbeier.git
- Student 1 UCI NetID:74445067
- Student 1 Name:Xiaobin Lin
- Student 2 UCI NetID (if applicable):
- Student 2 Name (if applicable):

### 2. Catalog information about Index

- Show your catalog information about an index (tables, columns).  
  another index catalog to store index info, format as table name + attribute name + index file  
  every index files info will store in index catalog instead of in table's table. Table's table only store info about
  index catalog and index catalog's column info will store in Column's table.

### 3. Filter

- Describe how your filter works (especially, how you check the condition.)  
  while loop check if record fit in this filter condition, if yes, return the record.  
  For condition checking, it is likely to scan condition check. Basically form key and compare to value.

### 4. Project

- Describe how your project works.  
  I write two funtions to extract all fields data and re-form data fields. So I firstly extract data and then choose
  attributes and corresponding data to rebuild projected data.

### 5. Block Nested Loop Join

- Describe how your block nested loop join works (especially, how you manage the given buffers.)  
  Load R table into a map in memory, if records size exceed numPages * PAGE_SIZE, then stop loading data. loop check if
  S table's record can be joined, when S is consumed, load R again until R is consumed.

### 6. Index Nested Loop Join

- Describe how your index nested loop join works.  
  for every record in R table, check if it is in S's index, if yes joined two records. Resetting index scan condition
  can be used to check S table's index.

### 7. Grace Hash Join (If you have implemented this feature)

- Describe how your grace hash join works (especially, in-memory structure).

### 8. Aggregation

- Describe how your basic aggregation works.  
  calculate all ops when traversing R table, output the result

- Describe how your group-based aggregation works. (If you have implemented this feature)

### 9. Implementation Detail

- Have you added your own module or source file (.cc or .h)? Clearly list the changes on files and CMakeLists.txt, if
  any.  
  yes, in qe folder, I created a file named tools.cpp


- Other implementation details:

### 10. Member contribution (for team of two)

- Explain how you distribute the workload in team.

### 11. Other (optional)

- Freely use this section to tell us about things that are related to the project 4, but not related to the other
  sections (optional)


- Feedback on the project to help improve the project. (optional)