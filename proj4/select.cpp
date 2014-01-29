#include "catalog.h"
#include "query.h"
#include "index.h"

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Select(const string & result,      // name of the output relation
	                 const int projCnt,          // number of attributes in the projection
		         const attrInfo projNames[], // the list of projection attributes
		         const attrInfo *attr,       // attribute used inthe selection predicate 
		         const Operator op,          // predicate operation
		         const void *attrValue)      // literal value in the predicate
{
    Status status;
    AttrDesc *attr_desc = NULL;

    /* If there is a Select Condition => Populate the attr_desc object */
    if(attr) {
	attr_desc = new AttrDesc;
	status = attrCat->getInfo(attr->relName, attr->attrName, *attr_desc);
	if(status != OK) {
	    if(attr_desc) delete attr_desc;
	    return status;
	}
    }

    /* Find the length of a resulting record and populate the proj_names array */
    int reclen;
    AttrDesc *proj_names = new AttrDesc[projCnt];
    status = ConvertInfoToDesc(projNames, projCnt, proj_names, reclen);
    if(status != OK) {
	if(attr_desc) delete attr_desc;
	delete [] proj_names;
	return status;
    }

    /* Check Optimization Criteria and Call Scan Method */
    if(attr_desc && (op == EQ) && (attr_desc->indexed))
	status = IndexSelect(result, projCnt, proj_names, attr_desc, op, attrValue, reclen);
    else
	status = ScanSelect(result, projCnt, proj_names, attr_desc, op, attrValue, reclen);

    if(attr_desc) delete attr_desc;
    delete [] proj_names;

    if(status != OK)
	return status;
    return OK;
}

