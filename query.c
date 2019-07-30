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
	int depth;
	Bits start;   // start bucket value
	Tuple qtuple;

	Count unnum;      //count how many unknown bits in the certain depth
	Count ctuple;     // number of tuples scanned in curpage
	Bits option;    // cur combination of unknown bits
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
	char buf[MAXBITS+1]; // for debug
	new->rel = r;
	Count nvals = nattrs(r);
	char *vals[nvals];
	tupleVals(q,vals);

	int cmp[nvals];
	Bits qknow = 0xFFFFFFFF;
	Bits nknow = 0x00000000;
	Bits mask = 0x00000000;
	Bits hash[nvals];
	for (int i = 0; i < nvals; i++)
	{
		if (vals[i] == NULL) fatal("Wrong number of attribute");
		cmp[i] = strcmp(vals[i], "?");
		if (!cmp[i]) {
			hash[i] = 0x00000000;
		} else hash[i] = hash_any((unsigned char *) vals[i], strlen(vals[i]));

		bitsString(hash[i],buf);
		printf("hash\"%s\" is %s\n",vals[i],buf);
	}

	ChVecItem *choiceVector = chvec(r);
	//Not tested
	for (int i = 0; i < MAXBITS; i++)
	{
		mask = 0x00000000;
		if (!cmp[choiceVector[i].att])
			nknow = setBit(nknow, i);
		mask = setBit(mask, choiceVector[i].bit);
		if ((hash[choiceVector[i].att] & mask) == 0)
			qknow = unsetBit(qknow, i);
	}

	new->known = qknow;
	new->unknown = nknow;
	bitsString(qknow,buf);
	printf("qknow is %s\n",buf);
	mask = 0x00000000;
	new->depth = depth(r);
	for (int i = 0; i < depth(r); ++i)
	{
		mask = setBit(mask, i);
	}

	PageID id = qknow & mask;

	if (id < splitp(r))
	{
		mask = setBit(mask, depth(r));
		id = qknow & mask;
		new->depth++;
	}

	int num = 0;
	for (int i = 0; i < MAXBITS - 1; i++)
	{
		if (i >= new->depth)
			break;
		if (nknow & (0x00000001 << i))
			num++;
	}
	new->curpage = id;
	new->is_ovflow = 0;
	new->curtup = 0;

	new->unnum = num;
	new->start = id;
	new->qtuple = copyString(q);


	new->ctuple = 0;


	new->option = 0x00000000;

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

	while (TRUE)
	{
		PageID pid = q->curpage;
		

		FILE *file;
		if (q->is_ovflow)
		{
			file = ovflowFile(r);
		} else
		{
			file = dataFile(r);
		}

		Page p = getPage(file, pid);
		//scan the cur page until there is no left tuples
		//return if find match
		while (q->ctuple < pageNTuples(p))
		{
			Tuple tmp = nextTuple(file,q->curpage,q->curtup);
			q->ctuple++;
			q->curtup = q->curtup + strlen(tmp) + 1;
			//printf("all tuples : %s   %d\n", tmp, strlen(tmp));	//debug
			if (tupleMatch(q->rel, q->qtuple, tmp))
				return tmp;
			free(tmp);
		}

		//switch to next page or overflow

		if (pageOvflow(p) != -1)
		{
			q->curpage = pageOvflow(p);
			q->ctuple = 0;
			q->curtup = 0;
			q->is_ovflow = 1;
			continue;
		} else
		{
			Bits uknow = q->unknown;
			Bits op = q->option;
			op++;
			int i;
			int count = q->unnum;

			if (op >= (0x00000001 << count))
			{
				// including three cases:
				// 1) op from 0x11111111 to 0x00000000, count == 8
				// 2) op from 0x00000000 to 0x00000001, count == 0 (no unknown bits)
				// 3) op from 0x0000000F to 0x00000010, count == 4 (normal case)
				break;
			}
			q->option = op;

			int offset = 0;
			Bits mask = 0;
			for (i = 0; i < count; ++i)
			{
				while (!(uknow & (setBit(0, offset))))
					offset++;

				if (op & setBit(0, i))
					mask = setBit(mask, offset);

				offset++;
			}
			PageID id = q->start | mask;

			if(id > npages(r)-1)
				break;

			q->curpage = id;
			q->ctuple = 0;
			q->curtup = 0;
			q->is_ovflow = 0;

		}
	}

	return NULL;
}



// clean up a QueryRep object and associated data

void closeQuery(Query q)
{
	// TODO
	free(q);

}
