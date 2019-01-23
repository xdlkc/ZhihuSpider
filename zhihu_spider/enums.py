from enum import Enum


class TopicEnum(Enum):
    over_topic_id_set = 'over_topic_id_set'
    over_children_topic_id_set = 'over_children_topic_id_set'
    topic_id_to_db_id_hash = 'topic_id_to_db_id_hash'
