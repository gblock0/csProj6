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

    cout << "in insert" << endl;
    
    ifs= new InsertFileScan(relation, status);
    if(status != OK) return status;
    
    cout << "after ifs creation" << endl;
    
  //Check to make sure there are attributes in the attrList
  if(attrCnt == 0 || sizeof(attrList) == 0){
    /* **************NEED TO CHANGE to another error********** */
    return FILEEOF;
  }

    cout << "after check on attributes in attrList" << endl;

  //Get attribute data
  status = attrCat->getRelInfo(relation, realCnt, attrs);
  if (status != OK) return status;

    cout << "got rel info" << endl;
    
  //calculate size of a new tuple
    tupleSize = 0;
    for(int i= 0; i < realCnt; i++)
    {
        tupleSize += attrs[i].attrLen;
    }
    
    cout << "after size calculation" << endl;
    
  tuplePtr = malloc(tupleSize);
    //need to add malloc error status 
    if(tuplePtr == NULL)ASSERT(false);
  
    cout << "after malloc" << endl;
    
  tupleOffset = 0; 
  for(int i = 0; i < realCnt; i++){
    for(int j = 0; j < attrCnt; j++){
      strComp = strcmp(attrs[i].attrName, attrList[j].attrName); 
      cout << " i dont think your like this" << endl;
      string s1(attrs[i].attrName);
      cout << s1<< endl;
      string s2(attrList[j].attrName);
      cout << s2 << endl;
      cout << attrList[j].attrLen << endl;
        
      if(strComp == 0){
        //get offset
        cout << "are you even here?" << endl;
          
        if(attrList[j].attrLen == NULL)
        {
          //need to make new return status
          cout << "null value" << endl;
          return FILEEOF;
        }
          
        tupleOffsetPtr = (void*) (((char*) tuplePtr) + tupleOffset);
        //copy data into new tuple
          cout << attrList[j].attrLen << endl;
        memcpy(tupleOffsetPtr, &(attrList[j].attrValue), attrList[j].attrLen);
          cout << "dont be like this" << endl;
        //add to offset
        tupleOffset += attrList[j].attrLen;
        break;
      }
    }
  }
  
    cout << "after tuple reation" << endl;
    
    record.data = tuplePtr;
    record.length = tupleSize;
    
    status = ifs->insertRecord(record, outRid);
    if (status != OK) return status;

    free(tuplePtr);
    delete ifs;
    delete attrs;

  return status;

}

