hu泽杰写的globalcache系统的日志代码部分
# Checkpoint做的事
# Recovery做的事
## 1. 从尾到头遍历日志
要应用的日志一定是从已写日志的末尾j_tail开始的，这个j_tail是以块为单位的，是建立在obj_store上层的块存储抽象。从j_tail得到j_oid_idx和off，也即其在底层对象存储中的地址。
### 1.1 读journal_header
journal_header的写对象存储应该是有保证不跨块的，从j_oid, off处指定读的长度读出到JournalHeaderl类型的journal_header
```C++
objstore->ObjRead(j_oid, (char*)&journal_header, sizeof(JournalHeader), off);
```
### 1.2 读各个描述块对象并应用日志
先根据jouenal_header.desc_num读出所有存储描述块的块。

然后遍历各个描述块得到desc_info，将脏页读出来并写到home location（desc_info里面包含这些信息）。

写完之后改下journal的超级块：jsb->j_tail和jsb->j_free的改动持久化。

### 1.3 如果应用了完了日志，journal超级块重置
重置超级块，并释放desc_buf和write_buf
```C++
if (need_update) {
    jsb->j_tail = 0;
    jsb->j_head = 0;
    jsb->j_free = JOURNAL_BLOCK_MAX_NUM;
    FlushJSB();
    if (desc_buf != nullptr) free(desc_buf);
    if (write_buf != nullptr) free(write_buf);
}
```

todo: 其实还可以整理下几个条件变量和函数之间的依赖关系