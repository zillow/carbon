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
import os
import threading
from operator import itemgetter
from random import choice
from collections import defaultdict

from carbon.conf import settings
from carbon import events, log
from carbon.pipeline import Processor

from carbon_index.index import CarbonIndex

try:
  import cPickle as pickle
except ImportError:
  import pickle


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

    # Signal to shutdown
    self.will_shutdown = False

    # Let's also keep track of unflush_counts for each metric
    self.metric_unflush_counts = defaultdict(int)

    # If we haven't received a metric for two hours,
    # let's drop its datapoints. Before dropping, make sure
    # its datapoints have been flushed.
    # {metric: timestamp}
    self.last_received_timestamps = defaultdict(int)

    # Build an index for carbon cache
    self.index = CarbonIndex()

    self.strategy = None
    if strategy:
      self.strategy = strategy(self)
    super(_MetricCache, self).__init__(dict)

  @property
  def counts(self):
    return [(metric, len(datapoints)) for (metric, datapoints) in self.items()]

  @property
  def unflush_counts(self):
    return [(metric, self._count_unflushed(metric)) for (metric, datapoints) in self.items()]

  @property
  def is_full(self):
    if settings.MAX_CACHE_SIZE == float('inf'):
      return False
    else:
      return self.size >= settings.MAX_CACHE_SIZE

  def _count_unflushed(self, metric):
    return self.metric_unflush_counts[metric]

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

    # 1. Retain last N datapoints in carbon cache,
    # let's push them back
    # 2. Do not push back during shutdown
    if not self.will_shutdown:
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

    # Update unflushed
    with self.lock:
      self.metric_unflush_counts[metric] -= len(datapoints)

    # Return directly during shutdown
    if self.will_shutdown:
      return (metric, datapoints)

    # 1) latest datatpoints receivced two hours ago or even older.
    # 2) all dataponts of that metric have been flushed already.
    if len(datapoints) == 0:
      time_now = int(time.time())
      oldest_timestamp = time_now - settings.MAX_TIME_GAP_FOR_MISSING_DATA
      if self.last_received_timestamps[metric] < oldest_timestamp:
        with self.lock:
          # remove the entry in unflush_counts map
          self.metric_unflush_counts.pop(metric, None)
          # remove the entry in last_received_timestamps map
          self.last_received_timestamps.pop(metric, None)
          # remove the index from CarbonIndex
          self.index.delete(metric)

        if metric in self:
          self.pop(metric)
          if settings.LOG_DROP_LATEST_DATAPOINTS:
            log.msg("MetricCache drops latest datapoints for metric {0}...".format(metric))

    return (metric, datapoints)

  def get_datapoints(self, metric):
    """Return a list of currently cached datapoints sorted by timestamp"""
    # let's keep old interface, strip out is_flushed before return
    return sorted(self.get_unsorted_datapoints(metric), key=lambda tup: tup[0])

  def get_unsorted_datapoints(self, metric):
    """Return a list of currently unsorted cached datapoints"""
    # let's keep old interface, strip out is_flushed before return
    return [(timestamp, value) for (timestamp, (value, is_flushed)) in self.get(metric, {}).items()]

  def expand_wildcard_query(self, metric):
    """ expand wildcard query """
    return self.index.expand_pattern(metric)

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
            self.metric_unflush_counts[metric] += 1

          if self.last_received_timestamps[metric] < timestamp:
            self.last_received_timestamps[metric] = timestamp

          # create an index for new metrics
          if len(self[metric]) == 0:
            self.index.insert(metric)
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


def _cleanup_files(files):
  for f in files:
    if os.path.isfile(f):
      os.unlink(f)


PICKLE_DUMP_PATH = os.environ.get("PICKLE_DUMP_PATH")
INSTANCE_NAME = os.environ.get("CARBON_CACHE_INSTANCE_NAME")


if PICKLE_DUMP_PATH and INSTANCE_NAME:

  instance_pickle_file_path = os.path.join(PICKLE_DUMP_PATH, "carbon-instance-{}.pickle".format(INSTANCE_NAME))
  metric_unflush_counts_pickle_file_path = os.path.join(PICKLE_DUMP_PATH, "carbon-instance-{}-metric_unflush_counts.pickle".format(INSTANCE_NAME))
  last_received_timestamps_pickle_file_path = os.path.join(PICKLE_DUMP_PATH, "carbon-instance-{}-last_received_timestamps.pickle".format(INSTANCE_NAME))
  carbon_index_pickle_file_path = os.path.join(PICKLE_DUMP_PATH, "carbon-instance-{}-carbon_index.pickle".format(INSTANCE_NAME))
  size_pickle_file_path = os.path.join(PICKLE_DUMP_PATH, "carbon-instance-{}-size.pickle".format(INSTANCE_NAME))

  pickled_files = [
    instance_pickle_file_path,
    metric_unflush_counts_pickle_file_path,
    last_received_timestamps_pickle_file_path,
    carbon_index_pickle_file_path,
    size_pickle_file_path,
  ]

  if os.path.isfile(instance_pickle_file_path):

    try:
      with open(instance_pickle_file_path, 'rb') as handle:
        MetricCache = pickle.load(handle)

      if os.path.isfile(metric_unflush_counts_pickle_file_path):
        with open(metric_unflush_counts_pickle_file_path, 'rb') as handle:
          MetricCache.metric_unflush_counts = pickle.load(handle)

      if os.path.isfile(last_received_timestamps_pickle_file_path):
        with open(last_received_timestamps_pickle_file_path, 'rb') as handle:
          MetricCache.last_received_timestamps = pickle.load(handle)

      if os.path.isfile(carbon_index_pickle_file_path):
        with open(carbon_index_pickle_file_path, 'rb') as handle:
          MetricCache.index = pickle.load(handle)

      if os.path.isfile(size_pickle_file_path):
        with open(size_pickle_file_path, 'rb') as handle:
          MetricCache.size = pickle.load(handle)

      MetricCache.strategy = write_strategy(MetricCache)
    except:
      MetricCache = _MetricCache(write_strategy)
    finally:
      # remove all pickled files
      _cleanup_files(pickled_files)

  else:
    MetricCache = _MetricCache(write_strategy)
else:
  MetricCache = _MetricCache(write_strategy)

# Avoid import circularities
from carbon import state
