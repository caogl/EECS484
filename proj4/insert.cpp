#include "catalog.h"
#include "query.h"
#include "index.h"
#include <stdio.h>
#include <string.h>

#include <iostream>
using std::cout;
using std::cerr;
using std::endl;

/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    Status status;

    int rel_attrCnt;
    AttrDesc *rel_attrs = NULL;
    status = attrCat->getRelInfo(relation, rel_attrCnt, rel_attrs);
    if(status != OK) {
	if(rel_attrs) delete [] rel_attrs;
	return status;
    }

    /* Check for equal number of attributes */
    if(attrCnt != rel_attrCnt) {
	delete [] rel_attrs;
	return ATTRTYPEMISMATCH;
    }

    /* Calculate the length of the Record to be Inserted */
    int rec_len = 0;
    for(int i=0; i<rel_attrCnt; i++)
	rec_len += rel_attrs[i].attrLen;

    /* Allocate space for the Record to be Inserted */
    char *rec_data = new char[rec_len]; 

    /* Match the attribute names and copy the data */
    for(int i=0; i<rel_attrCnt; i++) {
	int attr_idx;
	for(attr_idx = 0; attr_idx<attrCnt; attr_idx++) {
	    if(!strcmp(rel_attrs[i].attrName, attrList[attr_idx].attrName))
		break;
	}
	/* If didn't find the attribute name => Reject Insert */
	if((attr_idx == attrCnt)) {
	    delete [] rec_data;
	    delete [] rel_attrs;
	    return ATTRTYPEMISMATCH;
	}
	/* Copy the data */
	void *source = attrList[attr_idx].attrValue;
	void *destination = &rec_data[rel_attrs[i].attrOffset];
	int num_bytes = rel_attrs[i].attrLen;
	memcpy(destination, source, num_bytes);
    }

    /* Create the Record to be Inserted */
    Record new_rec = {rec_data, rec_len};
    
    /* Insert the Record into the HeapFile for the Relation */
    Status heap_status;
    RID rec_rid;
    HeapFile heap_file(relation, heap_status);
    status = heap_file.insertRecord(new_rec, rec_rid);
    if(status != OK) {
	delete [] rel_attrs;
	delete [] rec_data;
	return status;
    }


    /* Insert the RID into the existig Indexes */
    for(int i=0; i<rel_attrCnt; i++) {
	if(rel_attrs[i].indexed) {
	    const int offset = rel_attrs[i].attrOffset;
	    const int length = rel_attrs[i].attrLen;
	    const Datatype type = static_cast<Datatype>(rel_attrs[i].attrType);
	    const int unique = 0;
	    Status status;
	    Index index(relation, offset, length, type, unique, status);
	    if(status != OK) {
		delete [] rel_attrs;
		delete [] rec_data;
		return status;
	    }
	    void *value = &rec_data[offset];
	    status = index.insertEntry(value, rec_rid);
	    if(status != OK) {
		delete [] rel_attrs;
		delete [] rec_data;
		return status;
	    }
	}
    }

    delete [] rec_data;
    delete [] rel_attrs;
    return OK;
}

