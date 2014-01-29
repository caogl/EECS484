#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <string.h>

Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  cout << "Algorithm: Simple NL Join" << endl;
  Status status;
  string relName1 = attrDesc1.relName;
  string relName2 = attrDesc2.relName;

  /* Open the FileScan for the Outer Atribute (arbitrarily chose the attr2) */
  HeapFileScan file_scan_outer(relName2, status);
  if(status != OK) return status;

  /* Open the HeapFile for the Result Table */
  HeapFile result_heap_file(result, status);
  if(status != OK) return status;

  /* Scan Through the Outer Relation */
  RID rid_outer, rid_inner;
  Record record_outer, record_inner;
  while(file_scan_outer.scanNext(rid_outer, record_outer) == OK) {
      char *value = new char[attrDesc2.attrLen];
      char *source = ((char*)record_outer.data) + attrDesc2.attrOffset;
      memcpy(value, source, attrDesc2.attrLen);
      /* Open a FileScan for the Inner Attribute with a Predicate being the Outer Attribute */
      HeapFileScan file_scan_inner(relName1, attrDesc1.attrOffset, attrDesc1.attrLen,
				   static_cast<Datatype>(attrDesc1.attrType), value, op, status);
      if(status != OK) {
	  delete [] value;
	  return status;
      }
      /* Scan Through the Inner Relation */
      while(file_scan_inner.scanNext(rid_inner, record_inner) == OK) {
	  char *result_rec_data = new char[reclen];
	  Record result_rec = {result_rec_data, reclen};
	  int result_offset = 0;
	  /* Do the Projection On The Fly */
	  for(int i=0; i<projCnt; i++) {
	      char *source = NULL;
	      if(attrDescArray[i].relName == relName1)
		  source = ((char*)record_inner.data) + attrDescArray[i].attrOffset;
	      else
		  source = ((char*)record_outer.data) + attrDescArray[i].attrOffset;
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
      }
      
  }

  return OK;
}

