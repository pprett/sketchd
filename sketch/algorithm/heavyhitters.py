from collection import namedtuple

from sketch.algorithm import Sketch


Element = namedtuple('Element', 'id error bucket next')


class Bucket(object):

    def __init__(self, count, next_=None, previous=None):
        self.count = count
        self.next = next_
        self.previous = previous
        self.first_child = None
        self.n_children = 0

    def attach(self, element):
        first_child = self.first_child
        element.next = first_child
        self.first_child = element
        self.n_children += 1

    def detach(self, element):
        if self.first_child.id == element.id:
            rm_elem = self.first_child
            rm_elem.bucket = None
            self.first_child = rm_elem.next
        else:
            current = self.first_child
            while current:
                if current.id == element.id:
                    #TODO rm element

        self.n_children -= 1

    def __len__(self):
        return n_children



class StreamSummary(object):

    first_bucket = None
    elements = {}
    _min_elem = None

    def add(self, value):
        """Adds a new element to the summary if not exists. """
        if value not in self.elements:
            if not self.first_bucket or 1 < self.first_bucket.count:
                bucket = Bucket(1, previous=None, next=self.first_bucket)

            # create new element and add it to bucket 1 and dict.
            elem = Element(id=value, error=0)
            self.elements[value] = elem
            self.first_bucket.attach(elem)

    def incr(self, value):
        count_i = self.elements[value]
        bucket_i = count_i.bucket
        bucket_iplus = bucket_i.next
        bucket_i.detach(count_i)
        count_i.count += 1

        # find right bucket for count_i
        if bucket_iplus and count_i.count == bucket_iplus.count:
            bucket_iplus.attach(count_i)
        else:
            bucket = Bucket(count_i.count, next=bucket_iplus, previous=bucket_i)
            bucket.attach(count_i)
            bucket_i.next = bucket
            if bucket_iplus:
                bucket_iplus.previous = bucket

        if len(bucket_i) == 0:
            ## TODO rm bucket_i




    def peek(self):
        """Returns the ``value`` of the element with the smallest count. """
        return self.buckets[0].first().id

    def __in__(self, value):
        return value in self.elements


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

        print k, n, value, args

        if value in stream_summary:
            stream_summary.incr_counter(value)
        elif len(stream_summary) < k:
            stream_summary.add(value)
        else:
            min_value = stream_summary.peek()
            stream_summary.replace(min_value, value)
            stream_summary.incr_counter(value)
