#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>

/* 
 * A simple scan select using a heap file scan
 */

Status Operators::ScanSelect(const string& result,       // Name of the output relation
                             const int projCnt,          // Number of attributes in the projection
                             const AttrDesc projNames[], // Projection list (as AttrDesc)
                             const AttrDesc* attrDesc,   // Attribute in the selection predicate
                             const Operator op,          // Predicate operator
                             const void* attrValue,      // Pointer to the literal value in the predicate
                             const int reclen)           // Length of a tuple in the result relation
{
  cout << "Algorithm: File Scan" << endl;
  Status status;
  string relName = projNames[0].relName;

  /* If an Attribute is Specified, Start a Scan with a Predicate else Just Start a Regular Scan */
  HeapFileScan *file_scan = NULL;
  if(attrDesc)
      file_scan = new HeapFileScan(relName, attrDesc->attrOffset, attrDesc->attrLen,
				   static_cast<Datatype>(attrDesc->attrType), (char*)attrValue, op, status);
  else
      file_scan = new HeapFileScan(relName, status);

  if(status != OK) {
      if(file_scan) delete file_scan;
      return status;
  }

  /* Open the HeapFile for the Result Table */
  HeapFile result_heap_file(result, status);
  if(status != OK) {
      delete file_scan;
      return status;
  }

  /* Scan Through the Matching Records in the HeapFile */
  RID rid;
  Record record;
  while(file_scan->scanNext(rid, record) == OK) {
      char *result_rec_data = new char[reclen];
      Record result_rec = {result_rec_data, reclen};
      int result_offset = 0;
      /* Do the Projection On The Fly */
      for(int i=0; i<projCnt; i++) {
	  char *source = ((char*)record.data) + projNames[i].attrOffset;
	  char *destination = result_rec_data + result_offset;
	  int num_bytes = projNames[i].attrLen;
	  memcpy(destination, source, num_bytes);
	  result_offset += num_bytes;
      }
      /* Insert the Projected Record into the Result HeapFile */
      RID result_rec_rid;
      status = result_heap_file.insertRecord(result_rec, result_rec_rid);
      delete [] result_rec_data;
      if(status != OK) {
	  delete file_scan;
	  return status;
      }
  }
  delete file_scan;
  return OK;
}
