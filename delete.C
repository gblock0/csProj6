#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
/* This function will delete all tuples in relation satisfying the predicate specified by attrName, op, and the constant attrValue. 
 * type denotes the type of the attribute. You can locate all the qualifying tuples using a filtered HeapFileScan. */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
  Status status;
  HeapFileScan *hfs;
  AttrDesc record;
  RID rid;

  hfs = new HeapFileScan(relation, status);

  //Get the AttrDesc from the attrCat table
  status = attrCat->getInfo(relation, attrName, record);
  if (status != OK) return status;

  //Start HFS on the relation table
  status = hfs->startScan(record.attrOffset, record.attrLen, type, attrValue, op);
  if(status != OK) return status;

  while((status = hfs->scanNext(rid)) == OK){
    
    //Delete record if found in relation table
    status = hfs->deleteRecord();
    if(status != OK) return status;
  }
  
  //If end of file then return attribute not found
  if (status == FILEEOF){
    status = ATTRNOTFOUND;
  }

  return status;



}

