// reln.c ... functions on Relations
// part of Multi-attribute Linear-hashed Files
// Last modified by John Shepherd, July 2019

#include "defs.h"
#include "reln.h"
#include "page.h"
#include "tuple.h"
#include "chvec.h"
#include "bits.h"
#include "hash.h"

#define HEADERSIZE (3*sizeof(Count)+sizeof(Offset))

struct RelnRep {
	Count  nattrs; // number of attributes
	Count  depth;  // depth of main data file
	Offset sp;     // split pointer
    Count  npages; // number of main data pages
    Count  ntups;  // total number of tuples
	ChVec  cv;     // choice vector
	char   mode;   // open for read/write
	FILE  *info;   // handle on info file
	FILE  *data;   // handle on data file
	FILE  *ovflow; // handle on ovflow file
};

// create a new relation (three files)

Status newRelation(char *name, Count nattrs, Count npages, Count d, char *cv)
{
    char fname[MAXFILENAME];
	Reln r = malloc(sizeof(struct RelnRep));
	r->nattrs = nattrs; r->depth = d; r->sp = 0;
	r->npages = npages; r->ntups = 0; r->mode = 'w';
	assert(r != NULL);
	if (parseChVec(r, cv, r->cv) != OK) return ~OK;
	sprintf(fname,"%s.info",name);
	r->info = fopen(fname,"w");
	assert(r->info != NULL);
	sprintf(fname,"%s.data",name);
	r->data = fopen(fname,"w");
	assert(r->data != NULL);
	sprintf(fname,"%s.ovflow",name);
	r->ovflow = fopen(fname,"w");
	assert(r->ovflow != NULL);
	int i;
	for (i = 0; i < npages; i++) addPage(r->data);
	closeRelation(r);
	return 0;
}

// check whether a relation already exists

Bool existsRelation(char *name)
{
	char fname[MAXFILENAME];
	sprintf(fname,"%s.info",name);
	FILE *f = fopen(fname,"r");
	if (f == NULL)
		return FALSE;
	else {
		fclose(f);
		return TRUE;
	}
}

// set up a relation descriptor from relation name
// open files, reads information from rel.info

Reln openRelation(char *name, char *mode)
{
	Reln r;
	r = malloc(sizeof(struct RelnRep));
	assert(r != NULL);
	char fname[MAXFILENAME];
	sprintf(fname,"%s.info",name);
	r->info = fopen(fname,mode);
	assert(r->info != NULL);
	sprintf(fname,"%s.data",name);
	r->data = fopen(fname,mode);
	assert(r->data != NULL);
	sprintf(fname,"%s.ovflow",name);
	r->ovflow = fopen(fname,mode);
	assert(r->ovflow != NULL);
	// Naughty: assumes Count and Offset are the same size
	int n = fread(r, sizeof(Count), 5, r->info);
	assert(n == 5);
	n = fread(r->cv, sizeof(ChVecItem), MAXCHVEC, r->info);
	assert(n == MAXCHVEC);
	r->mode = (mode[0] == 'w' || mode[1] =='+') ? 'w' : 'r';
	return r;
}

// release files and descriptor for an open relation
// copy latest information to .info file

void closeRelation(Reln r)
{
	// make sure updated global data is put in info
	// Naughty: assumes Count and Offset are the same size
	if (r->mode == 'w') {
		fseek(r->info, 0, SEEK_SET);
		// write out core relation info (#attr,#pages,d,sp)
		int n = fwrite(r, sizeof(Count), 5, r->info);
		assert(n == 5);
		// write out choice vector
		n = fwrite(r->cv, sizeof(ChVecItem), MAXCHVEC, r->info);
		assert(n == MAXCHVEC);
	}
	fclose(r->info);
	fclose(r->data);
	fclose(r->ovflow);
	free(r);
}

// insert a new tuple into a relation
// returns index of bucket where inserted
// - index always refers to a primary data page
// - the actual insertion page may be either a data page or an overflow page
// returns NO_PAGE if insert fails completely
// TODO: include splitting and file expansion

PageID addToRelation(Reln r, Tuple t)
{
	int nTuples = r->ntups + 1; //add one tuple count for the new incoming tuple
	int nAttributes = r->nattrs;

	int pageCapacity = PAGESIZE/(10*nAttributes);

	if (nTuples % pageCapacity == 0) //split needed
		splitRelation(r);

	Bits h, p;
	char buf[MAXBITS+1];
	h = tupleHash(r,t);
	if (r->depth == 0)
		p = 1;
	else {
		p = getLower(h, r->depth);
		if (p < r->sp) p = getLower(h, r->depth+1);
	}
	bitsString(h,buf); printf("hash = %s\n",buf);
	bitsString(p,buf); printf("page = %s\n",buf);
	Page pg = getPage(r->data,p);
	if (addToPage(pg,t) == OK) {
		putPage(r->data,p,pg);
		r->ntups++;
		return p;
	}
	// primary data page full
	if (pageOvflow(pg) == NO_PAGE) {
		// add first overflow page in chain
		PageID newp = addPage(r->ovflow);
		pageSetOvflow(pg,newp);
		putPage(r->data,p,pg);
		Page newpg = getPage(r->ovflow,newp);
		// can't add to a new page; we have a problem
		if (addToPage(newpg,t) != OK) return NO_PAGE;
		putPage(r->ovflow,newp,newpg);
		r->ntups++;
		return p;
	}
	else {
		// scan overflow chain until we find space
		// worst case: add new ovflow page at end of chain
		Page ovpg, prevpg = NULL;
		PageID ovp, prevp = NO_PAGE;
		ovp = pageOvflow(pg);
		while (ovp != NO_PAGE) {
			ovpg = getPage(r->ovflow, ovp);
			if (addToPage(ovpg,t) != OK) {
				prevp = ovp; prevpg = ovpg;
				ovp = pageOvflow(ovpg);
			}
			else {
				if (prevpg != NULL) free(prevpg);
				putPage(r->ovflow,ovp,ovpg);
				r->ntups++;
				return p;
			}
		}
		// all overflow pages are full; add another to chain
		// at this point, there *must* be a prevpg
		assert(prevpg != NULL);
		// make new ovflow page
		PageID newp = addPage(r->ovflow);
		// insert tuple into new page
		Page newpg = getPage(r->ovflow,newp);
	        if (addToPage(newpg,t) != OK) return NO_PAGE;
        	putPage(r->ovflow,newp,newpg);
		// link to existing overflow chain
		pageSetOvflow(prevpg,newp);
		putPage(r->ovflow,prevp,prevpg);
        	r->ntups++;
		return p;
	}
	return NO_PAGE;
}

void splitRelation(Reln r) {
	//add a new page
	addPage(r->data);
	r->npages++;

	PageID pid = r->sp;
	FILE * f = r->data;
	Offset currTupleOffset = 0;
	Count nTuples = 0;

	//space for store tuples that should be in original page
	Tuple * origin = malloc(PAGESIZE * sizeof(Tuple));
	int i = 0;

	while (pid != NO_PAGE) {
		Page page = getPage(f, pid);
		if(pageNTuples(page) == 0) break; //no tuple in the page, break

		Tuple t = nextTuple(f, pid, currTupleOffset); //get current tuple

		// hash the current tuple and check its new pageID
		Bits hash = tupleHash(r, t);
		Bits newID = getLower(hash, r->depth + 1); //remember to add 1 for correct depth

		if (newID == pid) //if the tuple should remain at the original page
			origin[i++] = copyString(t);
		else // the tuple will be insert into new page
			if (insertIntoPage(r, t, newID) == NO_PAGE)
				fatal("tuple insertion to new page failed");
		currTupleOffset += strlen(t) + 1; //add '\0' position for the tuple, since it is simply a string
		nTuples ++; //add tuple count

		if (nTuples >= pageNTuples(page)) {//if there are tuples more than one page
			//add another page and change to overflow
			Page newpage = newPage();
			putPage(f, pid, newpage);
			pid = pageOvflow(page); //get overflow page id
			nTuples = 0; //reset tuple count
			currTupleOffset = 0; //reset tuple overflow
			f = r->ovflow; //point to overflow position
		}
		free(t); //free original tuple, they are already in origin storage
	}

	for (int j = 0; j < i; j++) { //put original page tuples into original
		Tuple temp = origin[j];
		if (insertIntoPage(r, temp, r->sp) == NO_PAGE) //insert into splitter page
			fatal("tuple insertion to original page failed");
		free(temp);//free out tuple
	}

	free(origin); //free the whole temp storage
	if (getLower(r->sp + 1, r->depth) != 0)
		r->sp++; //move split pointer
	else {
		r->depth++;
		r->sp = 0; //reset split pointer
	}
}

//insert to specific page helper function, modified from insert into relation
PageID insertIntoPage(Reln r, Tuple t, PageID pid) {
	Page page = getPage(r->data, pid);

	if (addToPage(page, t) == OK) {
		putPage(r->data, pid, page);
		return pid;
	}
	if(pageOvflow(page) == NO_PAGE) { //full of tuple, need overflow page
		PageID newPid = addPage(r->ovflow);
		pageSetOvflow(page, newPid); //need overflow page
		putPage(r->data, pid, page); //put the page in
		Page newPage = getPage(r->ovflow, newPid); //get overflow page
		if (addToPage(newPage, t) != OK) return NO_PAGE; //add error, return NO_PAGE
		putPage(r->ovflow, newPid, newPage); //insert into overflow page position
		return pid;
	} else { //have overflow page, go through until find a space to insert
		Page overflowPage, prevPage = NULL;
		PageID overflowPid, prevPid = NO_PAGE;
		overflowPid = pageOvflow(page);
		while( overflowPid != NO_PAGE ) { //traval through overflow page chain
			overflowPage = getPage(r->ovflow, overflowPid);
			if(addToPage(overflowPage, t) != OK) { //full, try next page
				prevPage = overflowPage; //update prev for record
				prevPid = overflowPid;
				overflowPid = pageOvflow(overflowPage); //get next overflow page
			} else { //have space, insert
				if (prevPage != NULL) free(prevPage); //free previous page
				putPage(r->ovflow, overflowPid, overflowPage); //add page into file
				return pid;
			}
		}
		//overflow chain is full, add a new overflow page
		PageID newPid = addPage(r->ovflow); //add page
		Page newPage = getPage(r->ovflow, newPid);
		if (addToPage(newPage, t) != OK) return NO_PAGE;
		putPage(r->ovflow, newPid, newPage); //put into overflow
		pageSetOvflow(prevPage, newPid); //link the overflow chain
		putPage(r->ovflow, prevPid, prevPage); //update the page
		return pid;
	}
	return NO_PAGE; //fatal error, return NO_PAGE
}
// external interfaces for Reln data

FILE *dataFile(Reln r) { return r->data; }
FILE *ovflowFile(Reln r) { return r->ovflow; }
Count nattrs(Reln r) { return r->nattrs; }
Count npages(Reln r) { return r->npages; }
Count ntuples(Reln r) { return r->ntups; }
Count depth(Reln r)  { return r->depth; }
Count splitp(Reln r) { return r->sp; }
ChVecItem *chvec(Reln r)  { return r->cv; }


// displays info about open Reln

void relationStats(Reln r)
{
	printf("Global Info:\n");
	printf("#attrs:%d  #pages:%d  #tuples:%d  d:%d  sp:%d\n",
	       r->nattrs, r->npages, r->ntups, r->depth, r->sp);
	printf("Choice vector\n");
	printChVec(r->cv);
	printf("Bucket Info:\n");
	printf("%-4s %s\n","#","Info on pages in bucket");
	printf("%-4s %s\n","","(pageID,#tuples,freebytes,ovflow)");
	for (Offset pid = 0; pid < r->npages; pid++) {
		printf("[%2d]  ",pid);
		Page p = getPage(r->data, pid);
		Count ntups = pageNTuples(p);
		Count space = pageFreeSpace(p);
		Offset ovid = pageOvflow(p);
		printf("(d%d,%d,%d,%d)",pid,ntups,space,ovid);
		free(p);
		while (ovid != NO_PAGE) {
			Offset curid = ovid;
			p = getPage(r->ovflow, ovid);
			ntups = pageNTuples(p);
			space = pageFreeSpace(p);
			ovid = pageOvflow(p);
			printf(" -> (ov%d,%d,%d,%d)",curid,ntups,space,ovid);
			free(p);
		}
		putchar('\n');
	}
}
