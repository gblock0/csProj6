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

  int attrCnt;
  AttrDesc *attrs;
  AttrDesc projNamesArray[projCnt];
  AttrDesc *attrDesc;
  int reclen = 0;
  int strcomp;

  attrDesc = (AttrDesc*) malloc(sizeof(AttrDesc));

  string bigTable(projNames[0].relName);


  //we need to get the attribute info for the result table 
  //so we add the correct attributes
  status = attrCat->getRelInfo(result, attrCnt, attrs);
  if (status != OK) return status;

  //Making the new projNames array for AttrDescs
  //converting the projNames attrInfo array into an array of AttrDesc
  for(int i = 0; i < projCnt; i++){
    for(int j = 0; j < attrCnt; j++){

      //Compares the two attrNames returns 0 if they are equal
      strcomp = strcmp(projNames[i].attrName, attrs[j].attrName);

      if( strcomp == 0){
        memcpy(&(projNamesArray[j]), &(attrs[j]), sizeof(AttrDesc));
        //length of a record in our projection talbe
        reclen += attrs[j].attrLen;
        //memcpy(&(projNamesArray[i].relName), &(projNames[i].relName), sizeof(projNames[i].relName));          
      }
    }
  }

  int bigTableAttrCnt;
  AttrDesc *bigTableAttrs;

  status = attrCat->getRelInfo(bigTable,bigTableAttrCnt, bigTableAttrs);

  //if attr is NULL we still need an attrDesc over so we can use relName
  if(attr == NULL){
    memcpy(attrDesc, &(projNames[0]), sizeof(AttrDesc));
  } else {  
    //else we need to find the matching attrDesc to convert from info to desc
    for(int i = 0; i < bigTableAttrCnt; i++){
      strcomp = strcmp(attr->attrName, bigTableAttrs[i].attrName);
      if(strcomp == 0)
      {
        memcpy(attrDesc, &(bigTableAttrs[i]), sizeof(AttrDesc));
      }
    }
  }

  status = ScanSelect(result, projCnt, projNamesArray, attrDesc, op, attrValue, reclen);

  delete attrs;
  delete bigTableAttrs;

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

  InsertFileScan *ifs;
  HeapFileScan *hfs;
  AttrDesc record;
  Record rec;
  Record newRec;
  RID rid;
  void *tuple;
  int attrCnt;
  AttrDesc *attrs;
  float tempFloat;  
  int tempInt;
  int strcomp;

  //convert from C string to C++ String
  string strBTRelName(attrDesc->relName);


  hfs = new HeapFileScan(strBTRelName, status);
  if(status != OK) return status;

  ifs = new InsertFileScan(result, status);
  if(status != OK) return status;


  //start scan seraching for the attrDesc that matches the filter and op


  //check attr type and cast accordingly
  if(attrDesc->attrType == INTEGER){
    tempInt = atoi(filter);
    filter = (char *) &tempInt;
  } else if(attrDesc->attrType == FLOAT){
    tempFloat = atof(filter);
    filter = (char *) &tempFloat;
  }

  status = attrCat->getRelInfo(strBTRelName, attrCnt, attrs);
  if (status != OK){
    return status;
  }


  status = hfs->startScan( attrDesc->attrOffset,attrDesc->attrLen,(Datatype) attrDesc->attrType, filter, op);
  if(status != OK){ 
    return status;
  }

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
      //return (status = "MALLOC ERROR");
      ASSERT(false);
    }

    int strcomp;
    int recOffset = 0;
    int tupleOffset = 0;
    //Matching projection atrribute into a new tuple
    for(int i = 0; i < projCnt; i++)
    {
      for(int j = 0; j < attrCnt; j++)
      {
        strcomp = strcmp(projNames[i].attrName, attrs[j].attrName);
        //something is wrong with the values of recOffset and tupleOffset 
        if(strcomp == 0)
        {
          //calculate offset into tuple
          recOffset = attrs[j].attrOffset;
          void *tupleOffsetPtr = (void *) (((char*) tuple) + tupleOffset);
          void *dataOffset = (void *) (((char *) rec.data) + recOffset);

          //copies the data from the original relation into the result relation
          memcpy(tupleOffsetPtr, dataOffset, projNames[i].attrLen);
          tupleOffset += projNames[i].attrLen; 
        }
      }
    }
    newRec.data = tuple;
    newRec.length = reclen;
    status = ifs->insertRecord(newRec, rid);
    if(status != OK) return status;
    free(tuple);
  }   

  //status was used as loop varient, reset status to be OK
  if(status == FILEEOF) status = OK;

  delete hfs;
  delete ifs;
  delete attrs; 

  return status;
}
