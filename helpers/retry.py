import logging
import time

LOG = logging.getLogger(__name__)


class Retry:
    """
    Decorator that that retries function until we get valid result

    :param exception_list: list of expected Exceptions
        If not in it, raise Exception, or pass if None
    :param exception_dict: dict of expected Exceptions and functions that should be called on
        that Exception
    :param retries: (int) number of times we will retry on Exception
        0 if we want it to run indefinite
    :param delay: (int) seconds we want to wait between each try
    :param max_delay: (int) max interval between retries
    :param multiplier: (int) multiplier for delay if we want increasing delay intervals

    :return: function result or raises error
    """

    def __init__(self,
                 custom_exception='',
                 exception_list=None,
                 exception_dict=None,
                 retries=0,
                 delay=1,
                 max_delay=0,
                 multiplier=2):

        self.exception_list = exception_list or []
        self.exception_dict = exception_dict
        self.retries = self.input_retries = retries
        self.delay = self.input_delay = delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self._exception = None
        self.custom_exception = custom_exception

    def __call__(self, f):
        def wrapped_f(*args, **kwargs):
            repeat_forever = self.retries == 0
            while self.retries > 0 or repeat_forever:
                try:
                    func_result = f(*args, **kwargs)
                except Exception as e:
                    self._exception = e
                    if self.exception_list and type(e) not in self.exception_list:
                        self.retries = self.input_retries
                        raise

                    if self.exception_dict and type(e) in self.exception_dict:
                        self.call_function_on_exception(type(e))
                    LOG.warning('Exception in {} raised : {!r}, trying again in {}s'.format(
                        f.__name__, e, self.delay))
                    self.retries -= 1
                    time.sleep(self.delay)
                    delay_multiplier = self.delay * self.multiplier
                    self.delay = self.max_delay if (
                        self.max_delay and self.max_delay < delay_multiplier
                    ) else delay_multiplier
                else:
                    self.delay = self.input_delay
                    self.retries = self.input_retries
                    return func_result
            else:
                if self.custom_exception and type(self._exception) in self.exception_list:
                    raise self.custom_exception
                else:
                    raise self._exception
        return wrapped_f

    def call_function_on_exception(self, _error):
        self.exception_dict[_error]()
