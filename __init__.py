import uuid
import zmq
import threading
import pickle
import logging
import time
import timeit

log = logging.getLogger(__name__)

class Processor(object):
    """Parallell processing class.
       Takes a list of Work objects to process.
       Instantiates ZeroMQ context and socket for receiving results.

       Takes an optional timeout in milliseconds that is applied to each wait inside the processing."""
    def __init__(self, items, timeout=None):
        self.socket_name = str(uuid.uuid1())
        self.items = items
        self.timeout = timeout
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind('inproc://%s' % self.socket_name)
        self.started = False
        self.finished = False
        self.results = {}

    def start(self):
        if self.started and not self.finished:
            raise ProcessingStartError
        else:
            self.finished = False
        self.started = True
        log.info("Starting parallel processing for %d items of work." % len(self.items))

        # @TODO: Ensure unique work.key values, otherwise results will be funny.
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
            worker.daemon = True
            worker.start()

    def wait_for_result(self):
        try:
            events = self.socket.poll(timeout=self.timeout)
            if events:
                frames = self.socket.recv_multipart()
                log.info("Received work thread response.")
                result = Result.unserialize(frames[2])
                self.results[result.key] = result
                if len(self.results) == len(self.items):
                    self.finished = True
                return result
            else:
                raise ProcessingTimeout("Timeout when waiting for next result.")
        except:
            self.close()
            log.exception("Caught exception in processing run:")
            raise

    def wait_for_all(self):
        timer_start = time.time()
        # @TODO: maybe replace with self.results?
        results = []
        log.info("Waiting for all results.")
        while len(results) < len(self.items):
            if self.timeout is None or time.time() - timer_start < self.timeout:
                result = self.wait_for_result()
                results.append(result)
            else:
                raise ProcessingTimeout("Timeout when waiting for all results.")

        log.info("Received all results.")
        return results

    def get_result(self, key):
        return self.results[key]

    def close(self):
        self.socket.close()
        self.context.term()


class Work(object):
    """A piece o work for processing.
    key should be a locally unique string.
    work_function should be a function to call.
    further args and kwargs will be passed unmodified to the worker thread.
    Keep thread-safety in mind."""
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


class ProcessingStartError(Exception):
    pass


class ProcessingTimeout(Exception):
    pass

def _work_wrapper(context, socket_name, key, work_function, *args, **kwargs):
    result = Result(key)
    try:
        value = work_function(*args, **kwargs)
        result.set_value(value)
        log.info("Normal result in worker thread.")
    except Exception as e:
        result.set_exception(e)
        log.exception("Caught exception result in worker thread. Returning exception result!")

    try:
        socket = context.socket(zmq.REQ)
        socket.connect('inproc://%s' % socket_name)
        socket.send(result.serialize())
        socket.close()
    except:
        log.exception("Caught exception result when sending result:")
        pass


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
    import sys
    log.addHandler(logging.StreamHandler(sys.stdout))
    log.setLevel(logging.INFO)

    log.info("Starting...")
    start_time = timeit.default_timer()

    processor = Processor([
        Work('0', _sample_work_0),
        Work('1', _sample_work_1),
        Work('3', _sample_work_3),
        Work('5', _sample_work, 5)
    ], timeout=500)

    processor.start()
    try:
        results = processor.wait_for_all()
    except Exception as e:
        log.exception("Caught exception in wait_for_all.")

    # processor = Processor([
    #     Work('0', _sample_work_0),
    #     Work('1', _sample_work_1),
    #     Work('3', _sample_work_3),
    #     Work('5', _sample_work, 5)
    # ])
    # log.info("Starting up new work.")
    # processor.start()

    # first_result = processor.wait_for_result()

    # log.info("First result was: %s" % str(first_result))

    # second_result = processor.wait_for_result()

    # log.info("Second result was: %s" % str(second_result))

    end_time = timeit.default_timer()
    log.info("Took %s seconds." % str(end_time - start_time))
