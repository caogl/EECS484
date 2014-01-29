#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cmath>
#include <cstring>

#define MAX(a,b) ((a) > (b) ? (a) : (b))
#define DOUBLEERROR 1e-07

Operator negate_operator(const Operator op) {
    switch(op) {
    case(LT): return GTE;
    case(LTE): return GT;
    case(EQ): return EQ;
    case(GTE): return LT;
    case(GT): return LTE;
    case(NE): return NE;
    case(NOTSET): return NOTSET;
    }
}

/*
 * Joins two relations
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Operators::Join(const string& result,           // Name of the output relation 
                       const int projCnt,              // Number of attributes in the projection
    	               const attrInfo projNames[],     // List of projection attributes
    	               const attrInfo* attr1,          // Left attr in the join predicate
    	               const Operator op,              // Predicate operator
    	               const attrInfo* attr2)          // Right attr in the join predicate
{
    Status status;

    /* Minirel guarantees there will be a Join Condition => Populate the attr_desc objects */
    AttrDesc attr_desc1, attr_desc2;
    status = attrCat->getInfo(attr1->relName, attr1->attrName, attr_desc1);
    if(status != OK) return status;
    status = attrCat->getInfo(attr2->relName, attr2->attrName, attr_desc2);
    if(status != OK) return status;

    /* Find the length of a resulting record and populate the proj_names array */
    int reclen;
    AttrDesc *proj_names = new AttrDesc[projCnt];
    status = ConvertInfoToDesc(projNames, projCnt, proj_names, reclen);
    if(status != OK) {
        delete [] proj_names;
        return status;
    }

    if(op != EQ)
	SNL(result, projCnt, proj_names, attr_desc1, op, attr_desc2, reclen);
    else {
	if(attr_desc1.indexed)
	    INL(result, projCnt, proj_names, attr_desc2, negate_operator(op), attr_desc1, reclen);
	else if(attr_desc1.indexed)
	    INL(result, projCnt, proj_names, attr_desc1, op, attr_desc2, reclen);
	else
	    SMJ(result, projCnt, proj_names, attr_desc1, op, attr_desc2, reclen);
    }

    return OK;
}

// Function to compare two record based on the predicate. Returns 0 if the two attributes 
// are equal, a negative number if the left (attrDesc1) attribute is less that the right 
// attribute, otherwise this function returns a positive number.
int Operators::matchRec(const Record& outerRec,     // Left record
                        const Record& innerRec,     // Right record
                        const AttrDesc& attrDesc1,  // Left attribute in the predicate
                        const AttrDesc& attrDesc2)  // Right attribute in the predicate
{
    int tmpInt1, tmpInt2;
    double tmpFloat1, tmpFloat2, floatDiff;

    // Compare the attribute values using memcpy to avoid byte alignment issues
    switch(attrDesc1.attrType)
    {
        case INTEGER:
            memcpy(&tmpInt1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(int));
            memcpy(&tmpInt2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(int));
            return tmpInt1 - tmpInt2;

        case DOUBLE:
            memcpy(&tmpFloat1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(double));
            memcpy(&tmpFloat2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(double));
            floatDiff = tmpFloat1 - tmpFloat2;
            return (fabs(floatDiff) < DOUBLEERROR) ? 0 : floatDiff;

        case STRING:
            return strncmp(
                (char *) outerRec.data + attrDesc1.attrOffset, 
                (char *) innerRec.data + attrDesc2.attrOffset, 
                MAX(attrDesc1.attrLen, attrDesc2.attrLen));
    }

    return 0;
}
