//
// Created by huzj on 10/12/22.
//

#include "page_cache.h"
#include <sys/sysinfo.h>
#include <glog/logging.h>

/**
 * we should use reference count since the page may be delete before set dirty for some update operations
 * @param page
 * @return
 */
bool PageRemovable(std::shared_ptr<Page>& page) {
    return (!page->Dirty() && !page->RefCount());
}

//void FreePage(std::shared_ptr<Page> page) {
//    if (page != nullptr) delete page;
//}

PageCache::PageCache(Journal* journal, ObjStore* objstore) {
    struct sysinfo s_info;
    int error;
    error = sysinfo(&s_info);
    if (error) LOG(ERROR) << "get sysinfo error";

    this->cache_capacity = (s_info.freeram / (AVERAGE_PAGE_SIZE*PAGECACHE_RATIO));
//    LOG(INFO) << "freeram: " << s_info.freeram;
    LOG(INFO) << "page cache capacity: " << this->cache_capacity;

    for (int i = 0; i < PAGECACHE_NUM; i++) {
        lru_cache[i] = new LRUCache<PageKey, std::shared_ptr<Page>>((this->cache_capacity / PAGECACHE_NUM), nullptr, PageRemovable);
        pthread_mutex_init(&this->pc_mutex[i], nullptr);
    }
    this->journal = journal;
    this->objstore = objstore;
}

PageCache::~PageCache() {
    //TODO should wait checkpoint
    LOG(INFO) << "pagecache desturct";
    for (int i = 0; i < PAGECACHE_NUM; ++i) {
//        LOG(WARNING) << "pc size: " << lru_cache[i]->GetSize();
        lru_cache[i]->Clear();
        pthread_mutex_destroy(&this->pc_mutex[i]);
        delete lru_cache[i];
    }
}


uint32_t PageCache::OID2Key(const ObjID& oid, uint32_t off) {
    uint32_t key;
    switch (oid.type) {
        case ObjType::ROOT:
            key = 0;
            break;
        case ObjType::FILE:
            key = oid.file_oid.ln;
            break;
        case ObjType::DIR:
//            key = oid.dir_oid.linear_idx;
            key = oid.dir_oid.ino.idx;

            break;
        case ObjType::INODE:
//            key = oid.inode_oid.id;
            key = off;
            break;
        case ObjType::FIQ:
            key = oid.fiq_oid.queue_idx;
            break;
        case ObjType::JROOT:
            key = 0;
            break;
        case ObjType::JBLOCK:
            key = oid.jb_oid.id;
//            break;
//        default:
//            key = 0;
    }
    return key;
}


/**
 * alloc the new page and get it, PutPage() should be used
 * @param oid
 * @param off
 * @param page_size
 * @return
 */
std::shared_ptr<Page> PageCache::AllocAndGetPage(const ObjID& oid, uint32_t off, uint32_t page_size) {
    std::shared_ptr<Page> page = nullptr;
    PageKey page_idx;
    OidToPageKey(oid, off, page_idx);

//    std::string page_idx = oid + ":";
//    page_idx += std::to_string(off);
    page = std::make_shared<Page>(page_size, oid, off);
//    if (this->objstore->ObjAccess(oid) == GCFSErr_PATHNOTEXISTS) {
//        this->objstore->ObjAlloc(oid);
        this->journal->AllocObj(oid);
//    }

    uint32_t pc_idx = (OID2Key(oid, off/page_size) % PAGECACHE_NUM);
    pthread_mutex_lock(&this->pc_mutex[pc_idx]);
    while (!lru_cache[pc_idx]->Contains(page_idx)) {
        int ret = lru_cache[pc_idx]->Insert(page_idx, page);
        if (ret) {
            if (journal->CheckPointListEmpty()) { //TODO all the page is dirty??? wait for commit and ck???
                //nothing to checkpoint, insert force temporarily
                ret = lru_cache[pc_idx]->Insert(page_idx, page, true);
                break;
            }
            pthread_mutex_unlock(&this->pc_mutex[pc_idx]);
            //cache full, checkpoint async to free pages
            this->journal->CallCheckpoint(true);
            this->journal->WaitForCheckpointDone();
            pthread_mutex_lock(&this->pc_mutex[pc_idx]);
        }
    }
    int ret = lru_cache[pc_idx]->Get(page_idx, page);
    page->Get();
    pthread_mutex_unlock(&this->pc_mutex[pc_idx]);
    return page;
}

/**
 *
 * @param oid
 * @param off
 * @param page_size
 * @param ref_page : if the page should be written and add ref count (default is true), if is true, PutPage() should be used
 * @return
 */
std::shared_ptr<Page> PageCache::GetPage(const ObjID& oid, uint32_t off, uint32_t page_size, bool ref_page) {
    std::shared_ptr<Page> page = nullptr;
    PageKey page_idx;
    OidToPageKey(oid, off, page_idx);

//    std::string page_idx = oid + ":";
//    page_idx += std::to_string(off);
    uint32_t pc_idx = (OID2Key(oid, off/page_size) % PAGECACHE_NUM);
    pthread_mutex_lock(&this->pc_mutex[pc_idx]);
    if (!lru_cache[pc_idx]->Contains(page_idx)) {
        while (!lru_cache[pc_idx]->Contains(page_idx)) {

            if (page == nullptr) {
                //alloc new page and read data from globalcache
                page = std::make_shared<Page>(page_size, oid, off);
//                if (this->objstore->ObjAccess(oid) == GCFSErr_PATHNOTEXISTS) {
//                    this->objstore->ObjAlloc(oid);
//                } else {
                    this->objstore->ObjRead(oid, page->data, page_size, off);
//                }
            }
//            std::cout << "GetPage fail, page_idx: " << page_idx << std::endl;
            int ret = lru_cache[pc_idx]->Insert(page_idx, page);
            if (ret) {
                //cache is full and no item can be delete
                if (journal->CheckPointListEmpty()) {
                    //nothing to checkpoint, insert force temporarily
                    ret = lru_cache[pc_idx]->Insert(page_idx, page, true);
                    break;
                }
                pthread_mutex_unlock(&this->pc_mutex[pc_idx]);
                //cache full, checkpoint async to free pages
                this->journal->CallCheckpoint(true);
                this->journal->WaitForCheckpointDone();
                pthread_mutex_lock(&this->pc_mutex[pc_idx]);
            }
        }
    }
    int ret = lru_cache[pc_idx]->Get(page_idx, page);
    if (ret) {
        //TODO not found, can't be appeared add LOG
    }
    //add reference count
    if (ref_page)
        page->Get();

    pthread_mutex_unlock(&this->pc_mutex[pc_idx]);
    return page;
}

void PageCache::PutPage(std::shared_ptr<Page>& page) {
    page->Put();
}