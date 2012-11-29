#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */


/* A selection is implemented using a filtered HeapFileScan.
 *   The result of the selection is stored in the result relation called result 
 *   (a  heapfile with this name will be created by the parser before QU_Select() is called). 
 *   The project list is defined by the parameters projCnt and projNames. 
 *   Projection should be done on the fly as each result tuple is being appended to the result table. 
 *   A final note: the search value is always supplied as the character string attrValue.  
 *   You should convert it to the proper type based on the type of attr. 
 *   You can use the atoi() function to convert a char* to an integer and atof() to convert it to a float. 
 *   If attr is NULL, an unconditional scan of the input table should be performed. */
const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
  Status status;
 // Qu_Select sets up things and then calls ScanSelect to do the actual work
  cout << "Doing QU_Select " << endl;


  /*
   * 1. Convert projnames to AttrDesc and convert attr to AttrDesc
   *  a. getRelInfo to get an attrs array which we can then check the attrName of the original arrays against
   *  b. If the attrName is the same add the info from attrs to the appropriate array
   * 2. convert attrValue to int and set to reclen using atoi
   * 3. Call ScanSelect
   */


  int attrCnt;
  AttrDesc *attrs;
  AttrDesc projNamesArray[projCnt];
  AttrDesc *attrDesc;
  status = attrCat->getRelInfo(attr[0].relName, attrCnt, attrs);
  if (status != OK) return status;

  //Making the new projNames array for AttrDescs
  for(int i = 0; i < attrCnt; i++){
    for(int j = 0; j < projCnt; j++){
      if(projNames[j].attrName == attrs[i].attrName){
        memcpy(&projNamesArray[i], &attrs[i], sizeof(attrs[i]));
      }
    }
  }

  //Making the new attr array for AttrDesc
  for(int i = 0; i < attrCnt; i++){
    for(int j = 0; j < sizeof(attr); j++){
      if(attr[j].attrName == attrs[i].attrName){
        memcpy(&attrDesc[i], &attrs[i], sizeof(attr[i]));
      }
    }
  }

  int reclen = atoi(attrValue);

  status = ScanSelect(result, projCnt, projNamesArray, attrDesc, op, attrValue, reclen);

  return status;
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
  cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
  Status status;

  HeapFileScan *hfs;
  AttrDesc record;
  RID rid;

  hfs = new HeapFileScan(attrDesc[0].relName, status);

  for(int i = 0; i < sizeof(attrDesc); i++){
    //Start HFS on the relation table
    status = hfs->startScan(attrDesc[i].attrOffset, attrDesc[i].attrLen, attrDesc[i].attrType, filter, op);
    if(status != OK) return status;

    while((status = hfs->scanNext(rid)) == OK){
      
      //Delete record if found in relation table
      status = hfs->deleteRecord();
      if(status != OK) return status;
    }
  }
  

}
