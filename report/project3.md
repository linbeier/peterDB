## Project 3 Report

### 1. Basic information

- Team #:
- Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-winter22-linbeier.git
- Student 1 UCI NetID:74445067
- Student 1 Name:Xiaobin Lin
- Student 2 UCI NetID (if applicable):
- Student 2 Name (if applicable):

### 2. Meta-data page in an index file

- Show your meta-data page of an index design if you have any. a seperate page with dummy node, stores root page number

### 3. Index Entry Format

- Show your index entry design (structure).

    - entries on internal nodes:  
      for fixed key type, entry: fixed 4 bytes for key and 4 bytes for page# for varchar type, entry: first 4 bytes for
      length, and followed len char and 4 bytes for page#
    - entries on leaf nodes:  
      for fixed key type, entry: fixed 4 bytes for key and 4 bytes for rid page#, 2 bytes for rid slot# for varchar
      type, entry: first 4 bytes for length, and followed len char and 4 bytes for rid page#, and 2 bytes for rid slot #

### 4. Page Format

- Show your internal-page (non-leaf node) design.  
  from start : page# | key | page# | key | page# ... from the end : key# | freeSpace | isLeaf flag key# and free space
  take 2 bytes, isLeaf flag takes 1 byte

- Show your leaf-page (leaf node) design.  
  from start: key | page# | key | page# ... from the end : key# | freeSpace | isLeaf flag | next page # | previous page
  # both next page and previous page take 4 bytes.

### 5. Describe the following operation logic.

- Split  
  leaf split: when an entry added to a full leaf node, leaf node will split to 2 leaf node , first d entries stays in
  origin page, last d entries move to next page . And copy and push middle key and new page number to newChildEntry.  
  internal split: when newChildEntry is not nullptr, it will check if current node have enough space, if yes, it will
  insert this newChildEntry to current internal node, if no, split current node and push the middle key and new node #
  to upper tree.


- Rotation (if applicable)


- Merge/non-lazy deletion (if applicable)


- Duplicate key span in a page  
  check if there is a key bigger than entry's key, if no insert to last, if yes insert entry to before this key.


- Duplicate key span multiple pages (if applicable)

### 6. Implementation Detail

- Have you added your own module or source file (.cc or .h)? Clearly list the changes on files and CMakeLists.txt, if
  any. yes, in ix folder, there is tools.cpp that is added.

- Other implementation details:

### 7. Member contribution (for team of two)

- Explain how you distribute the workload in team.

### 8. Other (optional)

- Freely use this section to tell us about things that are related to the project 3, but not related to the other
  sections (optional)


- Feedback on the project to help improve the project. (optional)
