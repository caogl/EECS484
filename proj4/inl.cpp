#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <string.h>

/* 
 * Indexed nested loop evaluates joins with an index on the 
 * inner/right relation (attrDesc2)
 */

Status Operators::INL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // Length of a tuple in the output relation
{
  cout << "Algorithm: Indexed NL Join" << endl;
  Status status;
  string relName1 = attrDesc1.relName;
  string relName2 = attrDesc2.relName;

  /* Open the FileScan for the Outer Atribute. This must be the one without an index in the case
   * that only one has an index. join.cpp ensures that that is attr1 in this case. */
  HeapFileScan file_scan_outer(relName1, status);
  if(status != OK) return status;

  /* Open the HeapFile for the Result Table */
  HeapFile result_heap_file(result, status);
  if(status != OK) return status;

  /* Scan Through the Outer Relation */
  RID rid_outer, rid_inner;
  Record record_outer, record_inner;
  while(file_scan_outer.scanNext(rid_outer, record_outer) == OK) {
      /* Open the Index on the Inner Attribute (attr2) */
      Datatype type = static_cast<Datatype>(attrDesc2.attrType);
      Index index_inner(relName2, attrDesc2.attrOffset, attrDesc2.attrLen, type, 0, status);
      if(status != OK) return status;

      /* Open a HeapFileScan object for the inner relation which we will need to get
       * Records from their RIDs using getRandomRecord */
      HeapFileScan file_scan_inner(relName2, status);
      if(status != OK) return status;

      /* Start the Scan through the Matching Records Using the Index */
      char *value = new char[attrDesc1.attrLen];
      char *source = ((char*)record_outer.data) + attrDesc1.attrOffset;
      memcpy(value, source, attrDesc1.attrLen);
      index_inner.startScan(value);
      while(index_inner.scanNext(rid_inner) == OK) {
	  file_scan_inner.getRandomRecord(rid_inner, record_inner);

          char *result_rec_data = new char[reclen];
          Record result_rec = {result_rec_data, reclen};
          int result_offset = 0;
          /* Do the Projection On The Fly */
          for(int i=0; i<projCnt; i++) {
              char *source = NULL;
              if(attrDescArray[i].relName == relName1)
                  source = ((char*)record_outer.data) + attrDescArray[i].attrOffset;
              else
                  source = ((char*)record_inner.data) + attrDescArray[i].attrOffset;
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
      index_inner.endScan();
  }
  return OK;
}

