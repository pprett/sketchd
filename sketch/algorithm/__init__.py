
class Sketch(object):
    """Base Sketch class. """

    def __init__(self, key):
        self.key = key

    def update(self, value, *args):
        raise NotImplementedError()
