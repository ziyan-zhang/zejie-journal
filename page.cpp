//
// Created by huzj on 10/12/22.
//


#include "page.h"
#include "transaction.h"

Page::Page(uint32_t page_size, const ObjID& oid, uint32_t off) {
    this->flags = 0;
    this->ref_count = 0;
    this->page_size = page_size;
    this->oid = oid;
    this->off = off;
    pthread_rwlock_init(&page_rw_lock, nullptr);
    data = (char*)malloc(page_size);
    modified = 0;
    frozen_data = nullptr;
    tx = nullptr;
    next_tx = nullptr;
}

Page::~Page() {
    SAFE_FREE(data);
    SAFE_FREE(frozen_data);
    pthread_rwlock_destroy(&page_rw_lock);
}

void Page::Get() {
    this->ref_count++;
}

void Page::Put() {
    this->ref_count--;
}

int Page::RefCount() {
    return this->ref_count;
}

void Page::LockPage(PageLock lock_type) {
    switch (lock_type) {
        case PAGE_WRITE:
            pthread_rwlock_wrlock(&this->page_rw_lock);
            break;
        case PAGE_READ:
            pthread_rwlock_rdlock(&this->page_rw_lock);
            break;
        default:
            break;    // 其实变成panic比较好，因为不该来这里
    }
}

void Page::UnlockPage() {
    pthread_rwlock_unlock(&this->page_rw_lock);
}

bool Page::Dirty() {
    return (this->flags & PAGE_DIRTY);
}

void Page::SetDirty() {
    this->flags |= PAGE_DIRTY;
}

void Page::ClearDirty() {
    this->flags &= (~PAGE_DIRTY);
}

bool Page::TxRunning() {
    return (this->flags & PAGE_TX_RUNNING);
}

void Page::SetTxRunning() {
    this->flags |= PAGE_TX_RUNNING;
}

void Page::ClearTxRunning() {
    this->flags &= (~PAGE_TX_RUNNING);
}

bool Page::NeedAddTxList() {
    return (this->flags & PAGE_NEED_ADD_TX_LIST);
}

void Page::SetAddTxList() {
    this->flags |= PAGE_NEED_ADD_TX_LIST;
}

void Page::ClearAddTxList() {
    this->flags &= (~PAGE_NEED_ADD_TX_LIST);
}

bool Page::AddNextRunningList() {
    return (this->flags & PAGE_ADD_NEXT_RUNNING_LIST);
}

void Page::SetNextAddRunningList() {
    this->flags |= PAGE_ADD_NEXT_RUNNING_LIST;
}

void Page::ClearNextAddRunningList() {
    this->flags &= (~PAGE_ADD_NEXT_RUNNING_LIST);
}
