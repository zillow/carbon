"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import time
import threading
from operator import itemgetter
from random import choice
from collections import defaultdict

from carbon.conf import settings
from carbon import events, log
from carbon.pipeline import Processor


def by_timestamp((timestamp, (value, is_flushed))):  # useful sort key function
  return timestamp


class CacheFeedingProcessor(Processor):
  plugin_name = 'write'

  def process(self, metric, datapoint):
    MetricCache.store(metric, datapoint)
    return Processor.NO_OUTPUT


class DrainStrategy(object):
  """Implements the strategy for writing metrics.
  The strategy chooses what order (if any) metrics
  will be popped from the backing cache"""
  def __init__(self, cache):
    self.cache = cache

  def choose_item(self):
    raise NotImplemented


class NaiveStrategy(DrainStrategy):
  """Pop points in an unordered fashion."""
  def __init__(self, cache):
    super(NaiveStrategy, self).__init__(cache)

    def _generate_queue():
      while True:
        metric_names = self.cache.keys()
        while metric_names:
          yield metric_names.pop()

    self.queue = _generate_queue()

  def choose_item(self):
    return self.queue.next()


class MaxStrategy(DrainStrategy):
  """Always pop the metric with the greatest number of points stored.
  This method leads to less variance in pointsPerUpdate but may mean
  that infrequently or irregularly updated metrics may not be written
  until shutdown """
  def choose_item(self):
    metric_name, size = max(self.cache.items(), key=lambda x: len(itemgetter(1)(x)))
    return metric_name


class RandomStrategy(DrainStrategy):
  """Pop points randomly"""
  def choose_item(self):
    return choice(self.cache.keys())


class SortedStrategy(DrainStrategy):
  """ The default strategy which prefers metrics with a greater number
  of cached points but guarantees every point gets written exactly once during
  a loop of the cache """
  def __init__(self, cache):
    super(SortedStrategy, self).__init__(cache)

    def _generate_queue():
      while True:
        t = time.time()
        metric_counts = sorted(self.cache.unflush_counts, key=lambda x: x[1])
        if settings.LOG_CACHE_QUEUE_SORTS:
          log.msg("Sorted %d cache queues in %.6f seconds" % (len(metric_counts), time.time() - t))
        while metric_counts:
          yield itemgetter(0)(metric_counts.pop())

    self.queue = _generate_queue()

  def choose_item(self):
    return self.queue.next()


class _MetricCache(defaultdict):
  """A Singleton dictionary of metric names and lists of their datapoints"""
  def __init__(self, strategy=None):
    self.lock = threading.Lock()
    self.size = 0
    # This helps to check if there is any unflushed datapoints in memcache,
    # helps to avoid infinity loop in writer. otherwise, needs O(n) to check.
    self.total_unflushed = 0
    self.strategy = None
    if strategy:
      self.strategy = strategy(self)
    super(_MetricCache, self).__init__(dict)

  @property
  def counts(self):
    return [(metric, len(datapoints)) for (metric, datapoints) in self.items()]

  @property
  def unflush_counts(self):
    return [(metric, self._count_unflushed(datapoints)) for (metric, datapoints) in self.items()]

  @property
  def is_full(self):
    if settings.MAX_CACHE_SIZE == float('inf'):
      return False
    else:
      return self.size >= settings.MAX_CACHE_SIZE

  @property
  def all_flushed(self):
    return self.total_unflushed == 0

  def _count_unflushed(self, datapoints):
    count = 0
    for timestamp, tup in datapoints.iteritems():
      value, is_flushed = tup
      if not is_flushed:
        count += 1
    return count

  def _check_available_space(self):
    if state.cacheTooFull and self.size < settings.CACHE_SIZE_LOW_WATERMARK:
      log.msg("MetricCache below watermark: self.size=%d" % self.size)
      events.cacheSpaceAvailable()

  def drain_metric(self):
    """Returns a metric and it's datapoints in order determined by the
    `DrainStrategy`_"""
    if not self:
      return (None, [])
    if self.strategy:
      metric = self.strategy.choose_item()
    else:
      # Avoid .keys() as it dumps the whole list
      metric = self.iterkeys().next()

    # Do not pop all datapoints for this metric.
    # Instead, retain last N datapoints in carbon cache.
    datapoints = self.pop(metric)

    # retain last N datapoints in carbon cache,
    # let's push them back
    for i in range(1, settings.MAX_RETAINED_LATEST_DATAPOINTS + 1):
      if i > len(datapoints):
        break
      timestamp, tup = datapoints[-i]
      value, is_flushed = tup
      self.store(metric, (timestamp, value), is_flushed=True)

    # Actions before returning
    # 1. filter out datapoints that have been already flushed.
    # 2. strip out is_flushed field, as for keeping same interface as before
    datapoints = [(timestamp, value) for (timestamp, (value, is_flushed)) in datapoints if not is_flushed]

    # Update total_unflushed
    with self.lock:
      self.total_unflushed -= len(datapoints)

    return (metric, datapoints)

  def get_datapoints(self, metric):
    """Return a list of currently cached datapoints sorted by timestamp"""
    # let's keep old interface, strip out is_flushed before return
    return sorted(self.get_unsorted_datapoints(metric), key=lambda tup: tup[0])

  def get_unsorted_datapoints(self, metric):
    """Return a list of currently unsorted cached datapoints"""
    # let's keep old interface, strip out is_flushed before return
    return [(timestamp, value) for (timestamp, (value, is_flushed)) in self.get(metric, {}).items()]

  def pop(self, metric):
    with self.lock:
      datapoint_index = defaultdict.pop(self, metric)
      self.size -= len(datapoint_index)
    self._check_available_space()

    return sorted(datapoint_index.items(), key=by_timestamp)

  def store(self, metric, datapoint, is_flushed=False):
    timestamp, value = datapoint
    if timestamp not in self[metric]:
      # Not a duplicate, hence process if cache is not full
      if self.is_full:
        log.msg("MetricCache is full: self.size=%d" % self.size)
        events.cacheFull()
      else:
        with self.lock:
          self.size += 1
          if not is_flushed: 
            self.total_unflushed += 1
          self[metric][timestamp] = (value, is_flushed)
    else:
      # Updating a duplicate does not increase the cache size
      self[metric][timestamp] = (value, is_flushed)


# Initialize a singleton cache instance
write_strategy = None
if settings.CACHE_WRITE_STRATEGY == 'naive':
  write_strategy = NaiveStrategy
if settings.CACHE_WRITE_STRATEGY == 'max':
  write_strategy = MaxStrategy
if settings.CACHE_WRITE_STRATEGY == 'sorted':
  write_strategy = SortedStrategy
if settings.CACHE_WRITE_STRATEGY == 'random':
  write_strategy = RandomStrategy

MetricCache = _MetricCache(write_strategy)

# Avoid import circularities
from carbon import state
