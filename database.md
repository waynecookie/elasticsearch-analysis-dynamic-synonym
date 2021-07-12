database: test
table: es_lexicon_synonym
```
-- auto-generated definition
create table es_lexicon_synonym
(
    lexicon_id      bigint auto_increment comment '词库id'
        primary key,
    lexicon_text    varchar(200) charset utf8          not null comment '同义词，词组，词与词之间使⽤英⽂逗号分割',
    lexicon_version int      default 0                 not null comment '版本号',
    lexicon_status  int      default 0                 not null comment '词条状态 0正常 1暂停使用',
    del_flag        int      default 0                 not null comment '作废标志 0正常 1作废',
    create_time     datetime default CURRENT_TIMESTAMP not null comment '创建时间'
)
    comment 'ES远程同义词库表' collate = utf8mb4_general_ci;
```