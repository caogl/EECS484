#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <string.h>


Status project_and_insert(const string &relName1,
			  const string &relName2,
			  Record &record1,
			  Record &record2,
			  HeapFile &result_heap_file,
			  int projCnt,
			  const AttrDesc attrDescArray[],
			  int reclen) {
    Status status;
    char *result_rec_data = new char[reclen];
    Record result_rec = {result_rec_data, reclen};
    int result_offset = 0;
    /* Do the Projection On The Fly */
    for(int i=0; i<projCnt; i++) {
	char *source = NULL;
	if(attrDescArray[i].relName == relName1)
	    source = ((char*)record1.data) + attrDescArray[i].attrOffset;
	else
	    source = ((char*)record2.data) + attrDescArray[i].attrOffset;
	char *destination = result_rec_data + result_offset;
	int num_bytes = attrDescArray[i].attrLen;
	memcpy(destination, source, num_bytes);
	result_offset += num_bytes;
    }
    /* Insert the Result Record into the Result HeapFile */
    RID result_rec_rid;
    status = result_heap_file.insertRecord(result_rec, result_rec_rid);
    delete [] result_rec_data;
    if(status != OK)
	return status;
    return OK;
}





/* Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  cout << "Algorithm: SM Join" << endl;
  Status status;
  string relName1 = attrDesc1.relName;
  string relName2 = attrDesc2.relName;

  /* Open the HeapFile for the Result Table */
  HeapFile result_heap_file(result, status);
  if(status != OK) return status;

  /* Open the Sorted File for Relation1 */
  Datatype type1 = static_cast<Datatype>(attrDesc1.attrType);
  int available_pgs = 0.8 * bufMgr->numUnpinnedPages();
  SortedFile sorted_file1(relName1, attrDesc1.attrOffset, attrDesc1.attrLen, type1, available_pgs, status);
  if(status != OK) return status;

  /* Open the Sorted File for Relation2 */
  Datatype type2 = static_cast<Datatype>(attrDesc2.attrType);
  available_pgs = 0.8 * bufMgr->numUnpinnedPages();
  SortedFile sorted_file2(relName2, attrDesc2.attrOffset, attrDesc2.attrLen, type2, available_pgs, status);
  if(status != OK) return status;

  /* Scan Through the Sorted Files */
  Record record1, record2;
  Status status1 = sorted_file1.next(record1);
  Status status2 = sorted_file2.next(record2);
  while((status1 == OK) && (status2 == OK)) {
      /* If Records Not Equal Increment the Smaller One */
      int comp = matchRec(record1, record2, attrDesc1, attrDesc2);
      if(comp < 0)
	  status1 = sorted_file1.next(record1);
      else if(comp > 0)
	  status2 = sorted_file2.next(record2);
      else {
	  /* Set a Mark to the Current Spot in the File2 */
	  Record tmp_record = record2;
	  sorted_file2.setMark();
	  /* While the Records are Equal => Insert to the Result HeapFile and Increment File2 */
	  do {
	      status = project_and_insert(relName1, relName2, record1, record2,
					  result_heap_file, projCnt, attrDescArray, reclen);
	      if(status != OK) return status;
	      status2 = sorted_file2.next(record2);
	  } while((status2 == OK) && (matchRec(record1, record2, attrDesc1, attrDesc2) == 0));
	  /* Increment File1. If the new File1 record is equal to the File2 record where the
	   * mark was set, go back to the mark for File2 */
	  status1 = sorted_file1.next(record1);
	  if((status1 == OK) && (matchRec(record1, tmp_record, attrDesc1, attrDesc2) == 0)) {
	      status = sorted_file2.gotoMark();
	      if(status != OK) return status;
	      status2 = sorted_file2.next(record2);
	  }
      }
  }
 
  return OK;
}


