create database zhihu;
use zhihu;
create table topic (
  id bigint auto_increment primary key,
  topic_id bigint not null unique comment '话题id',
  name varchar(128) not null comment '话题名称',
  url varchar(128) not null comment '话题地址',
  excerpt text not null comment '摘要',
  introduction text not null comment '概述',
  avatar_url varchar(255) not null comment '封面地址',
  type varchar(64),
  category varchar(64),
  is_black boolean default false,
  is_vote boolean default false,
  create_time timestamp null default current_timestamp,
  update_time timestamp null default current_timestamp on update current_timestamp,
  unique index (topic_id)
) engine = innodb comment '话题表';
create table topic_related (
  id bigint auto_increment primary key,
  topic_id bigint not null comment '当前话题id',
  children_id bigint not null comment '子话题id',
  create_time timestamp null default current_timestamp,
  update_time timestamp null default current_timestamp on update current_timestamp,
  foreign key (topic_id) references topic (id),
  foreign key (children_id) references topic (id)
) engine = innodb comment '父子话题关联表';
create table zhihu.question(
  id bigint auto_increment primary key,
  question_id bigint not null,
  question_created bigint not null,
  url varchar(256) not null,
  title text not null,
  question_type varchar(64) not null,
  type varchar(64) not null,
  create_time timestamp null default current_timestamp,
  update_time timestamp null default current_timestamp on update current_timestamp,
) engine = innodb comment '问题表';
create table zhihu.answer(
  id bigint auto_increment primary key,
  answer_id bigint not null,
  voteup_count int,
  excerpt text not null,
  content text not null,
  comment_count int not null,
  create_time timestamp null default current_timestamp,
  update_time timestamp null default current_timestamp on update current_timestamp,
) engine = innodb comment '回答表';
create table zhihu.article(
  id bigint auto_increment primary key,
  
  create_time timestamp null default current_timestamp,
  update_time timestamp null default current_timestamp on update current_timestamp,
)