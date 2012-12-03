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


    //what if attrValue == NULL
    
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
  int strcomp;
    
  attrDesc = (AttrDesc*) malloc(sizeof(AttrDesc));
    
    string bigTable(projNames[0].relName);
    cout << bigTable << endl;
    
    
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
        //this is slightly bootleg, because it just overwrites 
          memcpy(&(projNamesArray[i].relName), &(projNames[i].relName), sizeof(projNames[i].relName));
          //set offset
          
          
        }
      }
    }

  //if attr is NULL we still need an attrDesc over so we can use relName
  if(attr == NULL){
    memcpy(attrDesc, &(projNames[0]), sizeof(AttrDesc));
      cout << "in NULL" << endl;
  } else {  
    //else we need to find the matching attrDesc to convert from info to desc
      cout << "in loop" << endl;
    for(int i = 0; i < projCnt; i++){
      strcomp = strcmp(attr->attrName, projNamesArray[i].attrName);
        
        string s1(attr->attrName);
        string s2(projNamesArray[i].attrName);
        cout << s1 << endl;
        cout << s2<<endl;
        
      if(strcomp == 0){
          cout << "found something in loop" << endl;
        memcpy(attrDesc, &(projNamesArray[i]), sizeof(AttrDesc));
      }
    }
  }
    
  status = ScanSelect(result, projCnt, projNamesArray, attrDesc, op, attrValue, reclen);

  delete attrs;
    
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
    
    /*string goodName(projNames[0].relName);
  
    cout << strBTRelName << endl;
  cout << result << endl;
  cout << goodName << endl;*/
    
  hfs = new HeapFileScan(strBTRelName, status);
    cout << "here " << endl;
    cout << strBTRelName << endl;
    //ASSERT(false);
  if(status != OK) return status;
  
  ifs = new InsertFileScan(result, status);
  if(status != OK) return status;

    
  cout << "clear skys" << endl;
  //start scan seraching for the attrDesc that matches the filter and op
 
  if(filter != NULL)
  {
    
    cout << "SCAN THE WATERS" << endl;
      cout << strBTRelName << endl;
    cout << attrDesc->attrOffset << endl;
    cout << attrDesc->attrLen << endl;
    cout << (attrDesc->attrType) << endl;
    string s5(filter);
    cout << s5 << endl;
    cout << op << endl;
  }
    
  //check attr type and cast accordingly
  if(attrDesc->attrType == INTEGER){
    tempInt = atoi(filter);
    filter = (char *) &tempInt;
    cout << "in int" << endl;
  } else if(attrDesc->attrType == FLOAT){
        tempFloat = atof(filter);
        filter = (char *) &tempFloat;
        cout << "in float" << endl;
  }
   
  status = attrCat->getRelInfo(strBTRelName, attrCnt, attrs);
  if (status != OK){
    cout << "broken mast " << endl; 
    return status;
  }
    
    
    //boot leg fix for offset
    int offsetTest;
    for(int i = 0; i < attrCnt; i++){
        strcomp = strcmp(attrs[i].attrName, attrDesc->attrName);
        if (strcomp == 0) {
            offsetTest = attrs[i].attrOffset;
            //memcpy(&(attrDesc->attrOffset), (attrs[i].attrOffset), sizeof(attrDesc->attrOffset));
            cout << "new offset: " << offsetTest <<endl;
        }
    }
    
  status = hfs->startScan( (const int) offsetTest,attrDesc->attrLen,(Datatype) attrDesc->attrType, filter, op);
  if(status != OK){ 
    cout << "bad weather " << endl; 
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
    for(int i = 0; i < projCnt; i++)
    {
      for(int j = 0; j < attrCnt; j++)
      {
        strcomp = strcmp(projNames[i].attrName, attrs[j].attrName);
        //something is wrong with the values of recOffset and tupleOffset 
        if(strcomp == 0)
        {
            recOffset = attrs[j].attrOffset;
            //cout<< "tuple: " << tuple << endl;
            //cout << "tupple offset: " << tupleOffset << endl;
            //cout << "rec.data: " << (rec.data) << endl;
            //cout << "rec offset: " << recOffset << endl;
            //cout << "int i: " << i << " int j: " << j << endl;
            void *tupleOffsetPtr = (void *) (((char*) tuple) + tupleOffset);
          void *dataOffset = (void *) (((char *) rec.data) + recOffset);
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
    
    cout << "POOP DECK HAS BEEN SWABBED" << endl;
  return status;
}
