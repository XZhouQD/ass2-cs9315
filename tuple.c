// tuple.c ... functions on tuples
// part of Multi-attribute Linear-hashed Files
// Last modified by John Shepherd, July 2019

#include "defs.h"
#include "tuple.h"
#include "reln.h"
#include "hash.h"
#include "chvec.h"
#include "bits.h"

// return number of bytes/chars in a tuple

int tupLength(Tuple t)
{
	return strlen(t);
}

// reads/parses next tuple in input

Tuple readTuple(Reln r, FILE *in)
{
	char line[MAXTUPLEN];
	if (fgets(line, MAXTUPLEN-1, in) == NULL)
		return NULL;
	line[strlen(line)-1] = '\0';
	// count fields
	// cheap'n'nasty parsing
	char *c; int nf = 1;
	for (c = line; *c != '\0'; c++)
		if (*c == ',') nf++;
	// invalid tuple
	if (nf != nattrs(r)) return NULL;
	return copyString(line); // needs to be free'd sometime
}

Tuple nextTuple(FILE *in,PageID pid,Offset currTup)
{
	char tuple[MAXTUPLEN];
	//positionBase = pages * pagesize + 2*sizeof(int) for offset + 1*sizeof(int) for count
	//by keeping sizeof(Offset) instead of using sizeof(int), keep future development simple
	Offset postionBase = PAGESIZE * pid + 2 * sizeof(Offset) + sizeof(Count);

	//from the start of file, move to postionBase + current tuple
	fseek(in, postionBase + currTup, SEEK_SET);
	// save to file *in
	fgets(tuple, MAXTUPLEN - 1, in);

	//count how many fields we have
	char *i;
	int numberOfFields = 1;
	//start from the first, stop if empty
	for (i = tuple; *i !='\0'; i++)
	    //if find , so it is a seperate field. 
	    if(*i ==',')
	        numberOfFields++;
	return copyString(tuple); // needed to be free later
}
// extract values into an array of strings

void tupleVals(Tuple t, char **vals)
{
	char *c = t, *c0 = t;
	int i = 0;
	for (;;) {
		while (*c != ',' && *c != '\0') c++;
		if (*c == '\0') {
			// end of tuple; add last field to vals
			vals[i++] = copyString(c0);
			break;
		}
		else {
			// end of next field; add to vals
			*c = '\0';
			vals[i++] = copyString(c0);
			*c = ',';
			c++; c0 = c;
		}
	}
}

// release memory used for separate attirubte values

void freeVals(char **vals, int nattrs)
{
	int i;
	for (i = 0; i < nattrs; i++) free(vals[i]);
}

// hash a tuple using the choice vector
// TODO: actually use the choice vector to make the hash

Bits tupleHash(Reln r, Tuple t)
{
	char buf[MAXBITS+1];
	Count nvals = nattrs(r);
	char **vals = malloc(nvals*sizeof(char *));
	assert(vals != NULL);
	tupleVals(t, vals);
	Bits hash[nvals];

	//hash each attribute, store in hash
	for (int i = 0; i < nvals; i++) {
		hash[i] = hash_any((unsigned char *)vals[i], strlen(vals[i]));
		bitsString(hash[i], buf);
		//printf("hash(%s) = %s\n", vals[i], buf);
	}

	//use choice vector to insert bits
	ChVecItem * choiceVector = chvec(r); //get choice vector from relation
	char newBuf[MAXBITS+1]; //new hash buffer with cv
	for (int i = 0; i < MAXBITS; i++) { //for each bit from cv
		Byte attr = choiceVector[i].att;
		Byte bit = choiceVector[i].bit;
		bitsString(hash[attr], buf);
		//remove space
		int j = 0, k = 0;
		for (j = 0; buf[j] != '\0'; j++)
			if (buf[j] != ' ')
				buf[k++] = buf[j];
		newBuf[MAXBITS-1-i] = buf[MAXBITS-1-bit]; //store relative bit in newBuffer
	}

	//newBuffer into hash bits
	Bits result = 0;
	for (int i = 0; i < MAXBITS; i++)
		if (newBuf[i] == '1')
			result = setBit(result, MAXBITS-1-i); //reverse storage of result

	bitsString(result, buf);
	printf("Choice Vector hash value of (%s): %s\n", vals[0], buf);

	return result;
}

// compare two tuples (allowing for "unknown" values)

Bool tupleMatch(Reln r, Tuple t1, Tuple t2)
{
	Count na = nattrs(r);
	char **v1 = malloc(na*sizeof(char *));
	tupleVals(t1, v1);
	char **v2 = malloc(na*sizeof(char *));
	tupleVals(t2, v2);
	Bool match = TRUE;
	int i;
	for (i = 0; i < na; i++) {
		// assumes no real attribute values start with '?'
		if (v1[i][0] == '?' || v2[i][0] == '?') continue;
		if (strcmp(v1[i],v2[i]) == 0) continue;
		match = FALSE;
	}
	freeVals(v1,na); freeVals(v2,na);
	return match;
}

// puts printable version of tuple in user-supplied buffer

void tupleString(Tuple t, char *buf)
{
	strcpy(buf,t);
}
