import datetime
from typing import List, Iterator, Optional, Tuple

import operator
from functools import reduce
from golem_messages import message
from os import path
from peewee import Model, DateTimeField, CharField, IntegerField, BlobField

from golem.database import GolemSqliteDatabase

ANY = object()


class NetworkMessageQueue:

    DATABASE = GolemSqliteDatabase(
        None,
        threadlocals=True,
        pragmas=(
            ('foreign_keys', True),
            ('busy_timeout', 1000),
            ('journal_mode', 'WAL')
    ))

    def __init__(self, datadir):
        if not NetworkMessageQueue.DATABASE:
            db_path = path.join(datadir, 'messages.db')
            NetworkMessageQueue.DATABASE.init(db_path)
            NetworkMessageQueue.DATABASE.connect()

    @classmethod
    def put(cls,
            node_id: str,
            msg_type: int,
            msg_slots: List[tuple],
            task_id: Optional[str] = None,
            subtask_id: Optional[str] = None) -> None:

        Message(
            node_id=node_id,
            msg_type=msg_type,
            msg_slots=msg_slots,
            task_id=task_id,
            subtask_id=subtask_id

        ).save(force_insert=True)

    @classmethod
    def get(cls,
            node_id: str,
            task_id: Optional[object] = ANY,
            subtask_id: Optional[object] = ANY,
            consume: bool = False) -> Iterator[message.base.Message]:

        """ Returns a message iterator.
            Can be used by an established TaskSession between ourselves and
            node_id to know what messages should be sent."""

        clauses = [(Message.node_id == node_id)]

        if task_id is not ANY:
            clauses.append((Message.task_id == task_id))
        if subtask_id is not ANY:
            clauses.append((Message.subtask_id == subtask_id))

        reduced_clauses = reduce(operator.and_, clauses)

        for msg in Message.select().where(reduced_clauses).iterator():

            yield msg
            if consume:
                msg.delete()

    @classmethod
    def waiting(cls) -> Iterator[Tuple[str, str, str]]:

        """ Returns iterator of (node_id, task_id, subtask_id) that has
            pending messages. Can be used by TaskServer to know which nodes it
            should connect to."""

        for msg in Message.select(Message.node_id, Message.task_id,
                                  Message.subtask_id).distinct().iterator():

            yield msg.node_id, msg.task_id, msg.subtask_id


class BaseMessageModel(Model):

    created_date = DateTimeField(default=datetime.datetime.now)

    class Meta:
        database = NetworkMessageQueue.DATABASE


class Message(BaseMessageModel):

    node_id = CharField(index=True)  # Receiving node

    type = IntegerField()  # Message type
    slots = BlobField()  # Message slots

    task_id = CharField(null=True)  # Task context
    subtask_id = CharField(null=True)  # Subtask context

    class Meta:
        database = NetworkMessageQueue.DATABASE
