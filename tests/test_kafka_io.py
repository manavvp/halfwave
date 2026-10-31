from producers.kafka_io import produce_records


class FakeMessage:
    def __init__(self, key: bytes):
        self._key = key

    def key(self):
        return self._key

    def partition(self):
        return 0

    def offset(self):
        return 0


class SmallQueueProducer:
    """Mimics librdkafka: produce() raises BufferError when the local queue is full;
    poll() sends what's queued and fires the delivery callbacks."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.queue = []
        self.sent = []
        self.full_raises = 0

    def produce(self, topic, key, value, on_delivery):
        if len(self.queue) >= self.capacity:
            self.full_raises += 1
            raise BufferError("Local: Queue full")
        self.queue.append((key, value, on_delivery))

    def poll(self, timeout):
        if timeout == 0:
            return 0  # like a busy network: nothing completes without waiting
        drained, self.queue = self.queue, []
        for key, value, cb in drained:
            self.sent.append(value)
            cb(None, FakeMessage(key))
        return len(drained)

    def flush(self, timeout):
        self.poll(timeout)
        return 0


def test_full_queue_waits_and_retries_instead_of_crashing():
    producer = SmallQueueProducer(capacity=3)
    records = [{"repo": f"r{i}", "n": i} for i in range(10)]
    errors = produce_records(
        producer,
        "radar.test.v1",
        records,
        key_field="repo",
        value_serializer=lambda r, ctx: str(r["n"]).encode(),
    )
    assert errors == []
    assert producer.full_raises > 0  # the queue really filled up
    assert producer.sent == [str(i).encode() for i in range(10)]  # all delivered, in order
