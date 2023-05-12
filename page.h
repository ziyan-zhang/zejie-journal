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

// PageKey包含两个uint64_t的高8字节和低8字节，用于标识一个page。、
// Pagekey中定义了==和<两个操作符，用于比较两个PageKey是否相等和大小。
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

// 将objID的node属性设为off，并拷贝到PageKey
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
    char* frozen_data;      // 旧page数据的拷贝，用于committing。如果running tx想要修改committing tx的page数据时，committing tx需要先拷贝一份旧数据
    uint32_t modified;      // 该页面已被当前running tx修改
    // 默认日志中，running tx不能有两个，但是running tx和committing tx可以各有一个
    Transaction* tx;            // 指向拥有该页面的tx（committing or running tx）的指针
    Transaction* next_tx;       // 指向当前runningtx，该running tx 正在修改一个 已经属于某committing tx 的页面。
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
