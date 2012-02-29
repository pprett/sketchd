from twisted.python import log

from ..algorithm import Sketch, StreamSummary


class SpaceSaving(Sketch):
    """Space-Saving algorithm by [Metwally et al. 2005] to find top-k
    elements in a data stream. """

    def __init__(self, key, *args, **kargs):
        super(SpaceSaving, self).__init__(key)
        self.stream_summary = StreamSummary()

    def update(self, value, *args):
        k = int(args[0]) if len(args) > 0 else 10

        stream_summary = self.stream_summary
        n = len(stream_summary)

        if value in stream_summary:
            stream_summary.incr(value)
        elif len(stream_summary) < k:
            stream_summary.add(value)
        else:
            min_value = stream_summary.peek()
            if min_value is None:
                print value, repr(stream_summary)
            stream_summary.replace(min_value, value)
            stream_summary.incr(value)



    ## @defer.inlineCallbacks
    ## def update(self, value, *args):
    ##     k = int(args[0]) if len(args) > 0 else 10
    ##     score = 0

    ##     t0 = datetime.now()
    ##     if not self.locked:
    ##         try:
    ##             self.locked = True
    ##             log.msg("Lock.aquired")
    ##             n = yield self.rd.zcard(self.key)

    ##             log.msg(k, n, value, args)
    ##             if n >= k:
    ##                 rank = yield self.rd.zrank(self.key, value)
    ##                 log.msg("rank(%s) = %s" % (value, rank))
    ##                 if rank is None:
    ##                     rem_value, score = yield self.rd.zrange(self.key, 0,
    ##                                                             0, withscores=True)
    ##                     _ = yield self.rd.zrem(self.key, rem_value)
    ##                     score = int(score)
    ##                     log.msg("Remove %s w/ score %d" % (rem_value, score))

    ##             new_score = yield self.rd.zincrby(self.key, score + 1, value)
    ##             log.msg("SET %s %d" % (value, int(new_score)))
    ##         finally:
    ##             self.locked = False
    ##             log.msg("Lock.released")
    ##     else:
    ##         reactor.callLater(0.1, self.update, value, *args)

    ##     log.msg("TIME: %d" % (datetime.now() - t0).microseconds)
