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
  int reclen = 0;

  //i think this should be done on the result table
  status = attrCat->getRelInfo(attr[0].relName, attrCnt, attrs);
  if (status != OK) return status;

  //Making the new projNames array for AttrDescs
  //converting the projNames attrInfo array into an array of AttrDesc
  //may not be needed, because what does ordering actually do?
        //we do need to convert them to attrDesc
  for(int i = 0; i < attrCnt; i++){
    for(int j = 0; j < projCnt; j++){
      if(projNames[j].attrName == attrs[i].attrName){
        memcpy(&projNamesArray[i], &attrs[i], sizeof(attrs[i]));
        //length of a record in our projection talbe
        reclen += attrs[i].attrLen;
      }
    }
  }

  //convert the attr
  for(int i = 0; i < attrCnt; i++){
    if(attr->attrName == attrs[i].attrName){
      memcpy(attrDesc, &attrs[i], sizeof(attrDesc));
      break;
    }
  }
    
  /*
   //we decided this wasn't needed because attrDesc isn't an array
  //Making the new attr array for AttrDesc
  for(int i = 0; i < attrCnt; i++){
    for(int j = 0; j < sizeof(attr); j++){
      if(attr[j].attrName == attrs[i].attrName){
        memcpy(&attrDesc[i], &attrs[i], sizeof(attr[i]));
      }
    }
  }
  */

  //int reclen = atoi(attrValue);
    
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
  Record rec;
  RID rid;
  int offset;
  void *tuple;

  hfs = new HeapFileScan(attrDesc->relName, status);
  if(status != OK) return status;

  //start scan seraching for the attrDesc that matches the filter and op
  status = hfs->startScan(attrDesc->attrOffset,attrDesc->length, attrDesc->attrType, filter, op);

  //white not at the end of the file
  while((status = hfs->scanNext(rid)) != FILEEOF)
  {
    if(status != OK) return status;
      
    //get the next tuple that matches our condition
    status = hfs->getRecord(rec);
    if(status != OK) return status;
    
    //get a piece of memeory the size of a record
    tuple = malloc(reclen);
    if (tuple == NULL) {
      //TO DO put in a malloc error for status
      return (status = "MALLOC ERROR");
    }
    
    //for each projection add the value into our tuple
    //assumes that projNames is in sorted order 
    //do some sort of loop that correctly matches the values needed from the
    //whole record to the values stated in project count. 
    //do this in the loop: "mempy(tuple+offset, proj[i].value, proj[i].legnth);
      //                    offset += offset proj[i].length;
      //then the new record will only the the needed info for the projects
      //but also note that the order of the information added to the tuple
      //must match the order that the result table was created with

      
      
      
  }
  //insert the record
    
  /*
  for(int i = 0; i < sizeof(attrDesc); i++){
    //Start HFS on the relation table
    status = hfs->startScan(attrDesc[i].attrOffset, attrDesc[i].attrLen, (Datatype) attrDesc[i].attrType, filter, op);
    if(status != OK) return status;

    //How to do the insert?
    while((status = hfs->scanNext(rid)) == OK){
      
      //do something
      //get record
      //have the tuples
      //Construct record into record memcpy things from tuple
      //instruct insertRecord
      if(status != OK) return status;
    }
  }
   */
  

}
