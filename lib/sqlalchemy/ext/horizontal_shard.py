# ext/horizontal_shard.py
# Copyright (C) 2005-2017 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Horizontal sharding support.

Defines a rudimental 'horizontal sharding' system which allows a Session to
distribute queries and persistence operations across multiple databases.

For a usage example, see the :ref:`examples_sharding` example included in
the source distribution.

"""
from itertools import groupby

from ..orm.attributes import instance_state
from .. import util
from ..orm.session import Session
from ..orm.query import Query

__all__ = ['ShardedSession', 'ShardedQuery']


class ShardedQuery(Query):
    def __init__(self, *args, **kwargs):
        super(ShardedQuery, self).__init__(*args, **kwargs)
        self.id_chooser = self.session.id_chooser
        self.query_chooser = self.session.query_chooser
        self._shard_id = None

    def set_shard(self, shard_id):
        """return a new query, limited to a single shard ID.

        all subsequent operations with the returned query will
        be against the single shard regardless of other state.
        """

        q = self._clone()
        q._shard_id = shard_id
        return q

    def _execute_and_instances(self, context):
        def iter_for_shard(shard_id):
            context.attributes['shard_id'] = context.identity_token = shard_id
            result = self._connection_from_session(
                mapper=self._mapper_zero(),
                shard_id=shard_id).execute(
                context.statement,
                self._params)
            return self.instances(result, context)

        if self._shard_id is not None:
            return iter_for_shard(self._shard_id)
        else:
            partial = []
            for shard_id in self.query_chooser(self):
                partial.extend(iter_for_shard(shard_id))

            # if some kind of in memory 'sorting'
            # were done, this is where it would happen
            return iter(partial)

    def get(self, ident, **kwargs):
        if self._shard_id is not None:
            return super(ShardedQuery, self).get(ident)
        else:
            ident = util.to_list(ident)
            for shard_id in self.id_chooser(self, ident):
                o = self.set_shard(shard_id).get(ident)
                if o is not None:
                    return o
            else:
                return None


class ShardedSession(Session):
    def __init__(self, shard_chooser, id_chooser, query_chooser, shards=None,
                 query_cls=ShardedQuery, **kwargs):
        """Construct a ShardedSession.

        :param shard_chooser: A callable which, passed a Mapper, a mapped
          instance, and possibly a SQL clause, returns a shard ID.  This id
          may be based off of the attributes present within the object, or on
          some round-robin scheme. If the scheme is based on a selection, it
          should set whatever state on the instance to mark it in the future as
          participating in that shard.

        :param id_chooser: A callable, passed a query and a tuple of identity
          values, which should return a list of shard ids where the ID might
          reside.  The databases will be queried in the order of this listing.

        :param query_chooser: For a given Query, returns the list of shard_ids
          where the query should be issued.  Results from all shards returned
          will be combined together into a single listing.

        :param shards: A dictionary of string shard names
          to :class:`~sqlalchemy.engine.Engine` objects.

        """
        super(ShardedSession, self).__init__(query_cls=query_cls, **kwargs)
        self.shard_chooser = shard_chooser
        self.id_chooser = id_chooser
        self.query_chooser = query_chooser
        self.__binds = {}
        self.connection_callable = self.connection
        if shards is not None:
            for k in shards:
                self.bind_shard(k, shards[k])

    def connection(self, mapper=None, instance=None, shard_id=None, **kwargs):
        if shard_id is None:
            shard_id = self.shard_chooser(mapper, instance)

        if self.transaction is not None:
            return self.transaction.connection(mapper, shard_id=shard_id)
        else:
            return self.get_bind(
                mapper,
                shard_id=shard_id,
                instance=instance
            ).contextual_connect(**kwargs)

    def get_bind(self, mapper, shard_id=None,
                 instance=None, clause=None, **kw):
        if shard_id is None:
            shard_id = self.shard_chooser(mapper, instance, clause=clause)
        return self.__binds[shard_id]

    def bind_shard(self, shard_id, bind):
        self.__binds[shard_id] = bind

    def bulk_insert_mappings(self, mapper, mappings, return_defaults=False, bind_id=None):
        self._bulk_save_mappings(
            mapper, mappings, False, False, return_defaults, False, bind_id=bind_id)

    def bulk_update_mappings(self, mapper, mappings, bind_id=None):
        self._bulk_save_mappings(mapper, mappings, True, False, False, False, bind_id=bind_id)

    def bulk_save_objects(self, objects, return_defaults=False, update_changed_only=True, bind_id=None):
        for (mapper, isupdate, identity_token), states in groupby(
                (instance_state(obj) for obj in objects),
            lambda state: (state.mapper, state.key is not None, state.identity_token)
        ):
            self._bulk_save_mappings(
                mapper, states, isupdate, True,
                return_defaults, update_changed_only, identity_token)

    # def _bulk_save_mappings(self, mapper, mappings, isupdate, isstates, return_defaults, update_changed_only, bind_id):
        # if isstates:
        #     def state_bind(state):
        #         if bind_id is not None:
        #             return bind_id
        #         return state.identity_key
        #
        #     for bind_id, states in groupby(mappings, state_bind):
        #         super()._bulk_save_mappings(mapper, states, isupdate, isstates, return_defaults, update_changed_only, bind_id)
        # else:
        #     super()._bulk_save_mappings(mapper, mappings, isupdate, isstates, return_defaults, update_changed_only, bind_id)

