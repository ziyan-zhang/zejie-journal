//
// Created by huzj on 10/12/22.
//

#ifndef GCFS_PAGE_H
#define GCFS_PAGE_H


#include <pthread.h>
#include <string>
#include <memory>
#include <atomic>
#include "../gcfs.h"
#include "../toolkit/utils.h"

#define PAGE_DIRTY                  (1U)
#define PAGE_TX_RUNNING             (1U << 1)
#define PAGE_NEED_ADD_TX_LIST       (1U << 2)
#define PAGE_ADD_NEXT_RUNNING_LIST  (1U << 3)

struct PageKey {
    uint64_t page_key_h;
    uint64_t page_key_l;
    bool operator == (const PageKey& key) const {
        if (page_key_h == key.page_key_h && page_key_l ==key.page_key_l) return true;
        else return false;
    }

    bool operator < (const PageKey& key) const {
        if (page_key_h != key.page_key_h) return (page_key_h < key.page_key_h);
        else return (page_key_l < key.page_key_l);
    }
};

static inline void OidToPageKey(const ObjID& oid, uint32_t off, PageKey& page_key) {
    ObjID oid_temp = oid;
    oid_temp.node = off;
    memcpy(&page_key, &oid_temp, sizeof(ObjID));
}

enum PageLock {
    PAGE_WRITE, PAGE_READ,
};

class Transaction;
class Page {
private:
    pthread_rwlock_t page_rw_lock;
    uint32_t flags;
    std::atomic<int> ref_count;

public:
    uint32_t page_size;             //inode page,FIQ page 4K; dentry pages are larger (32K)

    char* data;
    char* frozen_data;              //the copy of the old page data for committing tx if the running tx want to modify the page data
    uint32_t modified;              //this flag signals the page has been modified by the current running tx
    Transaction* tx;                //pointer to transaction which owns this page (committing or running tx)
    Transaction* next_tx;           //pointer to the current running transaction which is modifying the page and the page is already in a committing tx
    ObjID oid;                      //obj id
    uint32_t off;                   //page offset in obj (in bytes)

    Page(uint32_t page_size, const ObjID& oid, uint32_t off);
    ~Page();
    void Get();
    void Put();
    int RefCount();
    void LockPage(PageLock lock_type);
    void UnlockPage();
    bool Dirty();
    void SetDirty();
    void ClearDirty();
    bool TxRunning();
    void SetTxRunning();
    void ClearTxRunning();
    bool NeedAddTxList();
    void SetAddTxList();
    void ClearAddTxList();
    bool AddNextRunningList();
    void SetNextAddRunningList();
    void ClearNextAddRunningList();
};


#endif //GCFS_PAGE_H
