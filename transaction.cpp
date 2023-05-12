//
// Created by huzj on 10/12/22.
//

#include "transaction.h"

// 初始化page_list_mutex、release_obj_set_mutex两个互斥量、ref_cond、
// 初始化block_num、ref_count两个数、tx_state一个状态
Transaction::Transaction() {
    pthread_mutex_init(&page_list_mutex, nullptr);
    pthread_mutex_init(&release_obj_set_mutex, nullptr);
    pthread_cond_init(&ref_cond, nullptr);
    block_num = 0;
    ref_count = 0;
    tx_state = TX_INIT;
}

// 清空dirty_pages、release_obj_set
// 销毁page_list_mutex、release_obj_set_mutex两个互斥量、ref_cond
Transaction::~Transaction() {
    dirty_pages.clear();
    release_obj_set.clear();
    pthread_cond_destroy(&ref_cond);
    pthread_mutex_destroy(&page_list_mutex);
    pthread_mutex_destroy(&release_obj_set_mutex);
}

// 将一个objId转换为一个release key，然后插入到release_obj_set中
void Transaction::ReleaseObjSetInsert(const ObjID& objId) {
    pthread_mutex_lock(&this->release_obj_set_mutex);
    this->release_obj_set.insert(OidToReleaseKey(objId));
    pthread_mutex_unlock(&this->release_obj_set_mutex);
}

// 在release_obj_set中查找objId，如果找到了，就删除它
bool Transaction::FindAndRemoveObj(const ObjID& objId) {
    std::set<ReleaseKey>::iterator iter;
    pthread_mutex_lock(&this->release_obj_set_mutex);
    iter = this->release_obj_set.find(OidToReleaseKey(objId));
    if (iter != this->release_obj_set.end()) {  // this->release_obj_set中找到了objId
        this->release_obj_set.erase(iter);
        pthread_mutex_unlock(&this->release_obj_set_mutex);
        return true;
    } else {
        pthread_mutex_unlock(&this->release_obj_set_mutex);
        return false;
    }
}
