#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

const Status BufMgr::allocBuf(int &frame)
{
    unsigned int startClockHand = clockHand; // Track full cycle
    do
    {
        advanceClock(); // Move clock hand (clockHand = (clockHand + 1) % numBufs)

        BufDesc *desc = &bufTable[clockHand];

        // Skip if pinned
        if (desc->pinCnt > 0)
        {
            continue;
        }

        // If recently referenced, clear refbit and skip
        if (desc->refbit)
        {
            desc->refbit = false;
            continue;
        }

        // Found a replaceable frame
        if (desc->valid)
        {
            // Write to disk if dirty
            if (desc->dirty)
            {
                Status status = desc->file->writePage(desc->pageNo, &bufPool[clockHand]);
                if (status != OK)
                {
                    return UNIXERR;
                }
                desc->dirty = false;
                bufStats.diskwrites++;
            }
            // Remove from hash table
            Status status = hashTable->remove(desc->file, desc->pageNo);
            if (status != OK)
            {
                return HASHTBLERROR;
            }
        }

        // Frame is free
        desc->Clear();
        frame = clockHand;
        return OK;
    } while (clockHand != startClockHand);

    // All frames pinned
    return BUFFEREXCEEDED;
}

const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status == OK)
    {
        // Page is in buffer pool
        BufDesc *desc = &bufTable[frameNo];
        desc->refbit = true;
        desc->pinCnt++;
        page = &bufPool[frameNo];
        bufStats.accesses++;
        return OK;
    }
    else if (status == HASHNOTFOUND)
    {
        // Page not in buffer pool
        status = allocBuf(frameNo);
        if (status != OK)
        {
            return status; // BUFFEREXCEEDED or UNIXERR
        }

        // Read page from disk
        status = file->readPage(PageNo, &bufPool[frameNo]);
        if (status != OK)
        {
            return UNIXERR;
        }

        // Update hash table and frame metadata
        status = hashTable->insert(file, PageNo, frameNo);
        if (status != OK)
        {
            return HASHTBLERROR;
        }

        BufDesc *desc = &bufTable[frameNo];
        desc->Set(file, PageNo); // pinCnt = 1, refbit = true, valid = true
        page = &bufPool[frameNo];
        bufStats.accesses++;
        bufStats.diskreads++;
        return OK;
    }
    return HASHTBLERROR; // Unexpected error from lookup
}

const Status BufMgr::unPinPage(File *file, const int PageNo, const bool dirty)
{
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if (status != OK)
    { // HASHNOTFOUND
        return HASHNOTFOUND;
    }

    BufDesc *desc = &bufTable[frameNo];
    if (desc->pinCnt <= 0)
    {
        return PAGENOTPINNED;
    }

    desc->pinCnt--;
    if (dirty)
    {
        desc->dirty = true;
    }
    return OK;
}

const Status BufMgr::allocPage(File *file, int &PageNo, Page *&page)
{
    // Allocate a new page in the file
    Status status = file->allocatePage(PageNo);
    if (status != OK)
    {
        return UNIXERR;
    }

    // Allocate a buffer frame
    int frameNo;
    status = allocBuf(frameNo);
    if (status != OK)
    {
        return status; // BUFFEREXCEEDED or UNIXERR
    }

    // Update hash table and frame metadata
    status = hashTable->insert(file, PageNo, frameNo);
    if (status != OK)
    {
        return HASHTBLERROR;
    }

    BufDesc *desc = &bufTable[frameNo];
    desc->Set(file, PageNo); // pinCnt = 1, refbit = true, valid = true
    page = &bufPool[frameNo];
    bufStats.accesses++;
    bufStats.diskreads++; // Consistent with project spec
    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
