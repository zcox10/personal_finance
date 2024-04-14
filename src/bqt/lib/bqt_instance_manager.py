class BqtInstanceManager(object):
    # This is a STATIC variable and should only be accessed as one
    # it hols all bqt objects currently being used so they can be accessed
    # without them being passed down to functions directly
    _bqt_instances = set()

    @classmethod
    def add(cls, instance):
        cls._bqt_instances.add(instance)

    @classmethod
    def delete(cls, instance):
        cls._bqt_instances.discard(instance)

    @classmethod
    def get_iff_one_exists(cls):
        """Returns a bqt object iff a single one is created

        NOTE: since the case is ambiguous when multiple bqt instances are
        created, this function will return None

        Returns:
            BqT object iff one instance is active, None otherwise
        """
        if len(cls._bqt_instances) == 1:
            return list(cls._bqt_instances)[0]
