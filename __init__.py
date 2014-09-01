import uuid
import zmq
import threading
import pickle
import logging

log = logging.getLogger(__name__)

class Processor(object):
    def __init__(self, items):
        self.socket_name = str(uuid.uuid1())
        self.items = items
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind('inproc://%s' % self.socket_name)

    def start(self):
        log.info("Starting parallel processing for %d items of work." % len(self.items))
        for work in self.items:
            thread_args = [
                self.context,
                self.socket_name,
                work.key,
                work.work_function
            ]
            thread_args.extend(work.args)
            worker = threading.Thread(target=_work_wrapper,
                args = thread_args,
                kwargs = work.kwargs
            )
            worker.start()

    def wait_for_response(self):
        frames = self.socket.recv_multipart()
        log.info("Received work thread response.")
        result = Result.unserialize(frames[2])
        return result

    def wait_for_all(self):
        results = []
        log.info("Waiting for all results.")
        while len(results) < len(self.items):
            result = self.wait_for_response()
            results.append(result)
        log.info("Received all results.")
        return results

    def close(self):
        self.socket.close()
        self.context.term()


class Work(object):
    def __init__(self, key, work_function, *args, **kwargs):
        self.key = key
        self.work_function = work_function
        if args:
            self.args = args
        else:
            self.args = ()
        if kwargs:
            self.kwargs = kwargs
        else:
            self.kwargs = {}


class Result(object):
    def __init__(self, key):
        self.key = key
        self.value = None
        self.exception = None

    def set_exception(self, exception):
        self.exception = exception

    def set_value(self, value):
        self.value = value

    @classmethod
    def unserialize(cls, serialized):
        result = pickle.loads(serialized)
        return result

    def serialize(self):
        return pickle.dumps(self)

    def __str__(self):
        if self.value:
            return str(self.value)
        if self.exception:
            return str(self.exception)
        return "Empty result"

    def __unicode__(self):
        return unicode(str(self))


def _work_wrapper(context, socket_name, key, work_function, *args, **kwargs):
    result = Result(key)
    try:
        value = work_function(*args, **kwargs)
        result.set_value(value)
        log.info("Normal result i worker thread.")
    except Exception as e:
        result.set_exception(e)
        log.exception("Caught exception result in worker thread. Returning exception result!")

    socket = context.socket(zmq.REQ)
    socket.connect('inproc://%s' % socket_name)
    socket.send(result.serialize())
    socket.close()

def _sample_work_0():
    return "Done 0 seconds."

def _sample_work_1():
    import time
    time.sleep(1)
    return "Done 1 second."

def _sample_work_3():
    import time
    time.sleep(3)
    return "Done 3 seconds."

def _sample_work(seconds):
    import time
    time.sleep(seconds)
    return "Done %d seconds." % seconds

if __name__ == '__main__':
    processor = Processor([
        Work('0', _sample_work_0),
        Work('1', _sample_work_1),
        Work('3', _sample_work_3),
        Work('5', _sample_work, 5)
    ])

    processor.start()

    results = processor.wait_for_all()

    processor.start()

    first_result = processor.wait_for_response()

    log.info("First result was: %s" % str(first_result))

    second_result = processor.wait_for_response()

    log.info("Second result was: %s" % str(second_result))
