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
  InsertFileScan* ifs;
  int strComp;
  int tupleSize;
  void *tuplePtr;
  int tupleOffset;
  void *tupleOffsetPtr;
  char *attrValue;
  int intCast;
  float floatCast;  
    
  ifs= new InsertFileScan(relation, status);
  if(status != OK) return status;
    
  //Check to make sure there are attributes in the attrList
  if(attrCnt == 0 || sizeof(attrList) == 0){
    /* **************NEED TO CHANGE to another error********** */
    return FILEEOF;
  }

  //Get attribute data
  status = attrCat->getRelInfo(relation, realCnt, attrs);
  if (status != OK) return status;
    
  //mismatch in # of attributes to be inserted and # of total attributes
  if(realCnt != attrCnt)
  {
      /* ********* need to make new error statement *****/
      return FILEEOF;
  }
    
  //calculate size of a new tuple
  tupleSize = 0;
  for(int i= 0; i < realCnt; i++)
  {
    tupleSize += attrs[i].attrLen;
  }
    
  tuplePtr = malloc(tupleSize);
  //need to add malloc error status 
  if(tuplePtr == NULL)ASSERT(false);
    
  //because the order of attrList may not match the order of the actual
  //layout of the attributes in the table we will do double loop to match up
  tupleOffset = 0; 
  for(int i = 0; i < realCnt; i++){
    for(int j = 0; j < attrCnt; j++){
      
      //the match between attrs and attrList
      strComp = strcmp(attrs[i].attrName, attrList[j].attrName); 
        
      if(strComp == 0){
          
        //does not support the NULL values NEED TO CHANGE RETURN VALUE
        if(attrList[j].attrValue == NULL) return FILEEOF;
        
        //calculate offset
        tupleOffsetPtr = (void*) (((char*) tuplePtr) + tupleOffset);
        
        //cast integer and float values
        switch (attrs[i].attrType) {
          case INTEGER:
            intCast = atoi((char*) attrList[j].attrValue);
            attrValue = (char *) &intCast;
            break;
          case FLOAT:
            floatCast = atof((char *)attrList[j].attrValue);
            attrValue = (char *) &floatCast;
            break;
          default:
            attrValue = (char*) attrList[j].attrValue;
            break;
        }
          
        //copy the value into the new tuple
        memcpy(tupleOffsetPtr, attrValue,  attrs[i].attrLen);
        
        tupleOffset += attrs[i].attrLen;
        break;
      }
    }
  }    
  
  //initalize record with tuple and size of tuple
  record.data = tuplePtr;
  record.length = tupleSize;
    
  //add the record to the table
  status = ifs->insertRecord(record, outRid);
  if (status != OK) return status;

  free(tuplePtr);
  delete ifs;
  delete attrs;

  return status;
}

