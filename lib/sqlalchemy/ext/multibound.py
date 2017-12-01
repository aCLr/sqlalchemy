from sqlalchemy.exc import UnboundExecutionError
from sqlalchemy.orm.base import _generative

from sqlalchemy.orm import Query, Session


class MultiBoundQuery(Query):
    @_generative()
    def set_bind_id(self, bind_id):
        self._bind_id = bind_id

    def _execute_and_instances(self, querycontext):
        querycontext.attributes['bind_id'] = self._bind_id
        result = self._connection_from_session(
            mapper=self._bind_mapper(),
            shard_id=self._bind_id
        ).execute(
            querycontext.statement,
            self._params
        )
        return self.instances(result, querycontext)


class MultiBoundSession(Session):
    def get_bind(self, mapper=None, clause=None, shard_id=None, **kwargs):
        original_bind = None
        try:
            original_bind = super(MultiBoundSession, self).get_bind(mapper=mapper, clause=clause)
        except UnboundExecutionError:
            pass
        bind_for_shard = self.__binds[shard_id]
        if original_bind.url == bind_for_shard.url:
            return original_bind
        else:
            return bind_for_shard

    def __init__(self, binds=None, query_cls=MultiBoundQuery, **kwargs):
        super(MultiBoundSession, self).__init__(query_cls=query_cls, **kwargs)
        self.__binds = {}
        if binds is not None:
            for k, v in binds.items():
                self.__binds[k] = v

    def add(self, instance, _warn=True, for_bind=None):
        return super().add(instance, _warn)
