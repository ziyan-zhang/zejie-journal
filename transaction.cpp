//
// Created by huzj on 10/12/22.
//

#include "transaction.h"

Transaction::Transaction() {
    pthread_mutex_init(&page_list_mutex, nullptr);
    pthread_mutex_init(&release_obj_set_mutex, nullptr);
    pthread_cond_init(&ref_cond, nullptr);
    block_num = 0;
    ref_count = 0;
    tx_state = TX_INIT;
}

Transaction::~Transaction() {
    dirty_pages.clear();
    release_obj_set.clear();
    pthread_cond_destroy(&ref_cond);
    pthread_mutex_destroy(&page_list_mutex);
    pthread_mutex_destroy(&release_obj_set_mutex);
}

void Transaction::ReleaseObjSetInsert(const ObjID& objId) {
    pthread_mutex_lock(&this->release_obj_set_mutex);
    this->release_obj_set.insert(OidToReleaseKey(objId));
    pthread_mutex_unlock(&this->release_obj_set_mutex);
}



bool Transaction::FindAndRemoveObj(const ObjID& objId) {
    std::set<ReleaseKey>::iterator iter;
    pthread_mutex_lock(&this->release_obj_set_mutex);
    iter = this->release_obj_set.find(OidToReleaseKey(objId));
    if (iter != this->release_obj_set.end()) {
        this->release_obj_set.erase(iter);
        pthread_mutex_unlock(&this->release_obj_set_mutex);
        return true;
    } else {
        pthread_mutex_unlock(&this->release_obj_set_mutex);
        return false;
    }
}
