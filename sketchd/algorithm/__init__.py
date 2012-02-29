

class Sketch(object):
    """Base Sketch class. """

    def __init__(self, key):
        self.key = key

    def update(self, value, *args):
        raise NotImplementedError()


class Element(object):

    def __init__(self, value=None, error=None, bucket=None, next=None):
        self.value = value
        self.error = error
        self.bucket = bucket
        self.next = next

    def __repr__(self):
        return "<Element value=%s, error=%s>" % (str(self.value), str(self.error))

    def __str__(self):
        return repr(self)


class Bucket(object):
    """A bucket holds values with equal count.

    Values are held in a single linked list.
    """

    def __init__(self, count, next=None, previous=None):
        self.count = count
        self.next = next
        self.previous = previous
        self.first_child = None
        self.n_children = 0

    def attach(self, element):
        """Attach the element to the front of the list.

        Postcond: ``element.bucket is self``. """
        first_child = self.first_child
        element.next = first_child
        element.bucket = self
        self.first_child = element
        self.n_children += 1

    def detach(self, element):
        """Removes element from the bucket's list. """
        previous = None
        current = self.first_child
        while current:
            if current.value == element.value:
                if previous is None:
                    self.first_child = current.next
                else:
                    previous.next = current.next
                current.bucket = None
                self.n_children -= 1
                return

            previous = current
            current = current.next

        raise KeyError("%s not in bucket" % str(element))

    def __len__(self):
        return self.n_children

    def __repr__(self):
        elements = []
        elem = self.first_child
        while elem:
            elements.append(repr(elem))
            elem = elem.next
        return ("<Bucket count=%d, n_children=%d, children=[" %
                (self.count, self.n_children)) + ", ".join(elements) + "]>"

    def __str__(self):
        return repr(self)


class StreamSummary(object):
    """In StreamSummary, all elements with the same counter value
    are linked together in a linked list. They all point to a
    parent ``Bucket``. The counter of the parent ``Bucket`` is
    the same as the counters' value of all its elements.
    Buckets are keept in a doubly linked list.
    """

    first_bucket = None
    last_bucket = None
    elements = {}

    def add(self, value):
        """Adds a new element to the summary if not exists. """
        if value not in self.elements:
            if not self.first_bucket or 1 < self.first_bucket.count:
                bucket = Bucket(1, previous=None, next=self.first_bucket)
                self.first_bucket = bucket
                if self.last_bucket is None:
                    self.last_bucket = bucket

            assert self.first_bucket is not None

            # create new element and add it to bucket 1 and dict.
            elem = Element(value=value, error=0)
            self.elements[value] = elem
            self.first_bucket.attach(elem)
        else:
            raise ValueError("%s already in StreamSummary. " % str(value))

    def incr(self, value):
        if value not in self.elements:
            raise KeyError("%s is not monitored" % str(value))
        count_i = self.elements[value]
        bucket_i = count_i.bucket
        assert bucket_i is not None
        bucket_iplus = bucket_i.next

        assert bucket_i.count > 0
        assert bucket_iplus is None or bucket_i.count < bucket_iplus.count

        bucket_i.detach(count_i)
        count_i_count = bucket_i.count + 1

        # find right ``bucket`` for count_i
        # it must be either ``bucket_iplus`` or a new one
        if bucket_iplus and count_i_count == bucket_iplus.count:
            bucket_iplus.attach(count_i)
            bucket = bucket_iplus
        else:
            bucket = Bucket(count_i_count, next=bucket_iplus, previous=bucket_i)
            bucket.attach(count_i)
            bucket_i.next = bucket
            if bucket_iplus:
                bucket_iplus.previous = bucket
            else:
                # we got a new last bucket
                self.last_bucket = bucket

        if len(bucket_i) == 0:
            assert bucket_i.first_child is None
            # remove bucket_i from summary
            if bucket_i is self.first_bucket:
                bucket.previous = None
                self.first_bucket = bucket
            else:
                bucket_i.previous.next = bucket
                bucket.previous = bucket_i.previous

            bucket_i.next = None
            bucket_i.previous = None
            del bucket_i

        assert count_i.bucket is not None

    def replace(self, old_value, new_value):
        """Replace the element with ``old_value`` with the value ``new_value``.

        Changes the value of ``self.elements[old_value]``. Resets the
        error of the element with the current counter value.
        Update elements dictionary.
        """
        assert new_value not in self.elements
        assert old_value in self.elements
        element = self.elements[old_value]
        element.error = element.bucket.count
        element.value = new_value
        del self.elements[old_value]
        self.elements[new_value] = element

    def peek(self):
        """Returns the ``Element`` with the smallest count.

        If there are multiple Elements choose the most recent one.
        """
        if not self.first_bucket or len(self.first_bucket) == 0:
            return None
        return self.first_bucket.first_child.value

    def __contains__(self, value):
        return value in self.elements

    def __len__(self):
        return len(self.elements)

    def __repr__(self):
        buckets = []
        bucket = self.first_bucket
        while bucket:
            buckets.append(repr(bucket))
            bucket = bucket.next
        return ("<StreamSummary n_elements=%d, buckets=[" % len(self)) \
               + "\n".join(buckets) + "]>"

    def __str__(self):
        return repr(self)
