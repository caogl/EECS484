#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>
Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
  cout << "Algorithm: Index Select" << endl;
  Status status;
  string relName = projNames[0].relName;

  /* Open the index on the specified attribute. The logic in select.cpp ensures that this method only
   * gets called with a non-null attrDesc ptr and that there is an index on the specified attribute */
  Datatype type = static_cast<Datatype>(attrDesc->attrType);
  Index index(relName, attrDesc->attrOffset, attrDesc->attrLen, type, 0, status);
  if(status != OK)
      return status;

  /* Open a HeapFileScan object which we will need to get Records from their RIDs using getRandomRecord */
  HeapFileScan file_scan(relName, status);
  if(status != OK)
      return status;

  /* Open the HeapFile for the Result Table */
  HeapFile result_heap_file(result, status);
  if(status != OK)
      return status;

  /* Scan through the Matching Records using the Index */
  RID rid;
  Record record;
  index.startScan(attrValue);
  while(index.scanNext(rid) == OK) {
      file_scan.getRandomRecord(rid, record);
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
	  //	  delete [] result_rec_data;
          return status;
      }
  }
  index.endScan();
  return OK;
}

