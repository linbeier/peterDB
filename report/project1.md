## Project 1 Report


### 1. Basic information
 - Team #:
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-winter22-linbeier.git
 - Student 1 UCI NetID:xiaobl3
 - Student 1 Name:Xiaobin Lin
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Internal Record Format
- Show your record format design.

  | Field number | offset | offset | data | data |
  |--------------|--------|--------|------|------|
  | 2 byte       | 2 byte | 2 byte |      |      |  
  
  Start with a field number, it indicates the number of fields in a record. Since one record can not larger than PAGE_SIZE, so 2 bytes are enough to store field number.  
  Then, the offset field stores the offset of the end of corresponding data. Total number of offset will be the value of field number.  
  Data is followed by the end of all offset fields.


- Describe how you store a null field.  
  when occur to null field, the offset of corresponding field will be set to 0. Since it is not possible to have a data ended at offset 0, offset 0 can be used as null value in offset field.  



- Describe how you store a VarChar field.  
  To store a VarChar field, the program first figure out the length of data, current offset of record buffer.
Then I will insert VarChar field data into record buffer(if free space available)
, and write the offset of the end of data to corresponding offset field. 


- Describe how your record design satisfies O(1) field access.  
The end of last data (stores in last data's offset field), is the beginning of current data field. So we will directly use last data's offset field as the specified data's begining, and offset field can be directly calculated. And that is O(1)   
 But if previous offset is 0(indicate null value), we need to go to the offset field before this 0 offset field until we find a offset field which is not 0 or we go to the start of offset fields.



### 3. Page Format
- Show your page format design.  
Within a page, records directly insert to the beginning of page, and slot directory grows from the end of the page, leaving free space in the middle.  
For pages in a file, pages grows from the beginning of file to the end and every 4096 bytes as a page.


- Explain your slot directory design if applicable.   

  | record offset | record len | record offset | record len | Record Num | Free Space |
  |---------------|------------|---------------|------------|------------|------------|  
  | 2 byte        | 2 byte     | 2 byte        | 2 byte     | 2 byte     | 2 byte     |  
  
  Slot directory grows from the end of a page, from the end of a page, there are free space, record number and all record slots(offset and len). Every number takes 2 bytes.  



### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.  
First check if current page has enough free space to store it, if not, check page from the first page to the page before current page. If found a page that can store that record, store it. If there is no page that have enough space to store it. 
Insert and initialize a new page and store the record. 


- How many hidden pages are utilized in your design?  
1 Hidden page used.


- Show your hidden page(s) format design if applicable  
Store 3 int value: read counter, write counter, append counter


### 5. Implementation Detail
- Other implementation details goes here.  
Since there is no delete page operation, so I organized pages within a file like an array. First is hidden page and then other pages. 


### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
