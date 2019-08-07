// query.c ... query scan functions
// part of Multi-attribute Linear-hashed Files
// Manage creating and using Query objects
// Last modified by John Shepherd, July 2019

#include "defs.h"
#include "query.h"
#include "reln.h"
#include "tuple.h"
#include "hash.h"

#define TRUE 1
#define FALSE 0
// A suggestion ... you can change however you like

struct QueryRep {
	Reln    rel;       // need to remember Relation info
	Bits    known;     // the hash value from MAH
	Bits    unknown;   // the unknown bits from MAH
	PageID  curpage;   // current page in scan
	int     is_ovflow; // are we in the overflow pages?
	Offset  curtup;    // offset of current tuple within page
	//TODO


	// add struct attributes
	int depth;    // linear hashing depth
	Bits start;   // the begin value, pageId
	Tuple qtuple; // string tuble

	Count unnum;      // the count umber of unkown bits in specific depth level
	Count ctuple;     // the count number of tuples gotten in current page
	Bits unbits;    // current unknow bits change level 
};

// take a query string (e.g. "1234,?,abc,?")
// set up a QueryRep object for the scan

Query startQuery(Reln r, char *q)
{
	Query new = malloc(sizeof(struct QueryRep));
	assert(new != NULL);
	// TODO
	// Partial algorithm:
	// form known bits from known attributes
	// form unknown bits from '?' attributes
	// compute PageID of first page
	//   using known bits and first "unknown" value
	// set all values in QueryRep object

	// initialize for some attributes
	new->is_ovflow = 0;
	new->curtup = 0;
	new->rel = r;
	new->depth = depth(r);
	new->ctuple = 0;
	new->unbits = 0;

	// preparation
	char buf[MAXBITS+1]; 
	Count nvals = nattrs(r);
	char *attr[nvals];
	tupleVals(q, attr);

	int cmp[nvals];
	// create compare masks

	Bits nknow = 0;
	Bits comp = 0;
	Bits hash[nvals];
	Bits qknow = 0xFFFFFFFF;
	for (int i = 0; i < nvals; i++) {
		if (attr[i] == NULL) fatal("Wrong number of attribute");
		cmp[i] = strcmp(attr[i], "?");
		if (!cmp[i]) {
			hash[i] = 0x00000000;
		// hash
		// equal
		} else hash[i] = hash_any((unsigned char *) attr[i], strlen(attr[i]));
		bitsString(hash[i],buf);
	}
	
	// for known/unknown
	ChVecItem *choiceVector = chvec(r);
	for (int i = 0; i < MAXBITS; i++) {
		// set
		if (!cmp[choiceVector[i].att])
			nknow = setBit(nknow, i);
		// mask set
		comp = 0;
		comp = setBit(comp, choiceVector[i].bit);
		if ((comp & hash[choiceVector[i].att]) == 0)
			qknow = unsetBit(qknow, i);
	}
	bitsString(qknow,buf);
	new->known = qknow;
	new->unknown = nknow;
	
	comp = 0;
	for (int i = 0; i < depth(r); i++) { comp = setBit(comp, i); }
	Count rs = splitp(r);
	// pageID set=> curr & start bucket value
	PageID start = qknow & comp;
	if (start < rs) {
		new->depth++;
		start = setBit(comp, depth(r)) & qknow;
	}
	new->curpage = start;
	new->start = start;
	// count unknow bits
	int counts = 0;
	for (int i = 0; i < MAXBITS - 1; i++) {
		int move = 1 << i;
		if (i >= new->depth) break; // out of range
		if (move & nknow) counts++;
	}
	new->unnum = counts;
	// compy query tuple string
	new->qtuple = copyString(q);

	return new;
}
// get next tuple during a scan

Tuple getNextTuple(Query q)
{
	// TODO
	// Partial algorithm:
	// if (more tuples in current page)
	//    get next matching tuple from current page
	// else if (current page has overflow)
	//    move to overflow page
	//    grab first matching tuple from page
	// else
	//    move to "next" bucket
	//    grab first matching tuple from data page
	// endif
	// if (current page has no matching tuples)
	//    go to next page (try again)
	// endif
	Reln r = q->rel;
	// loop condtion for valid check
	while (TRUE) {
		// get file
		FILE *f;
		if (q->is_ovflow != 1) f = dataFile(r);
		else f = ovflowFile(r);
		// get current page and the corresponding tuple number
		Page current = getPage(f, q->curpage);
		int overflow = pageOvflow(current);
		Count n = pageNTuples(current);

		//scan the cur page until there is no left tuples
		//return if find match
		while (q->ctuple < n) {
			Tuple next;
			q->ctuple++;
			next = nextTuple(f, q->curpage, q->curtup);
			int check = tupleMatch(q->rel, q->qtuple, next);
			q->curtup = q->curtup + strlen(next) + 1;
			if (check) return next;
			else free(next);
		}

		// check overflow
		// switch to next page
		if (overflow == -1) {
			// mask for set & compare
			Bits setM = 0;
			Bits uknow = q->unknown;
			int unnums = q->unnum;
			Bits uncount = q->unbits;

			// eg: 1 << 1 => 0x00000010
			int new_mask = 1 << unnums;

			// check condition
			// current unknow bits is equal or larger
			uncount++;
			if (new_mask <= uncount) {
				break;
			}
			// bit set position
			int pos = 0;
			for (int i = 0; i < unnums; i++) {
				while (!((setBit(0, pos) & uknow)))pos++;
				if (setBit(0, i) & uncount) setM = setBit(setM, pos);
				pos++;
			}
			// check  pages and mask
			if((npages(r)-1) < (q->start | setM)  )
				break;
			// updates attributes
			q->curpage = q->start | setM;
			q->is_ovflow = 0;
			q->curtup = 0;
			q->ctuple = 0;
			q->unbits = uncount;

		} else { // not -1 
			// updates attributes
			Offset ov = pageOvflow(current);
			q->is_ovflow = 1;
			q->curtup = 0;
			q->ctuple = 0;
			q->curpage = ov;
			continue;
		}
	}
	// next get nothing
	return NULL;
}



// clean up a QueryRep object and associated data
void closeQuery(Query q)
{
	// TODO
	free(q);

}
