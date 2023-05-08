//
// Created by huzj on 10/12/22.
//

#ifndef GCFS_PAGE_CACHE_H
#define GCFS_PAGE_CACHE_H


#include <pthread.h>
#include <string>
#include <memory>
#include <atomic>
#include "../toolkit/lru_cache.h"
#include "page.h"
#include "../objstore/objstore.h"
#include "journal.h"

#define AVERAGE_PAGE_SIZE 16384 //16K
#define PAGECACHE_RATIO (10) //TODO 10
#define PAGECACHE_NUM 16


class PageCache {
private:
    uint32_t cache_capacity;
    Journal* journal;
    ObjStore* objstore;
    pthread_mutex_t pc_mutex[PAGECACHE_NUM];
    LRUCache<PageKey, std::shared_ptr<Page>>* lru_cache[PAGECACHE_NUM];

    uint32_t OID2Key(const ObjID& oid, uint32_t off);

public:
    PageCache(Journal* journal, ObjStore* objstore);
    ~PageCache();
    std::shared_ptr<Page> AllocAndGetPage(const ObjID& oid, uint32_t off, uint32_t page_size);
    std::shared_ptr<Page> GetPage(const ObjID& oid, uint32_t off, uint32_t page_size, bool ref_page = true);
    void PutPage(std::shared_ptr<Page>& page);
};


#endif //GCFS_PAGE_CACHE_H
