#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

/* Insert a tuple with the given attribute values (in attrList) in relation. 
 *  The value of the attribute is supplied in the attrValue member of the attrInfo structure.
 *  Since the order of the attributes in attrList[] may not be the same as in the relation, you might have to rearrange them before insertion. 
 *  If no value is specified for an attribute, you should reject the insertion as Minirel does not implement NULLs. */
const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
  Status status;
  AttrDesc *attrs;
  Record record;
  RID outRid;
  RelDesc rd;
  int realCnt;
  InsertFileScan* ifs = new InsertFileScan(relation, status);
  if (status != OK) return status;


  //Check to make sure there are attributes in the attrList
  if(attrCnt == 0 || sizeof(attrList) == 0){
    /* **************NEED TO CHANGE********** */
    return status;
  }


  //Get attribute data
  status = attrCat->getRelInfo(relation, realCnt, attrs);
  if (status != OK) return status;


  for(int i = 0; i < sizeof(attrs); i++){
    for(int j = 0; j < sizeof(attrList); j++){
      if(attrs[i].attrName == attrList[j].attrName){
        
        /* 
        status = attrCat->getInfo(attrs[i].relName, attrs[i].attrName, attrOrdered[i]);
        if (status != OK) return status;
        */
  
        //Make the record of the attribute date
        record.data = attrList[j].attrValue;
        record.length = attrList[j].attrLen;

        //insert record into relation
        status = ifs->insertRecord(record, outRid);
        if (status != OK) return status;
        break;
      }
    }
  }
  



  return status;

}

