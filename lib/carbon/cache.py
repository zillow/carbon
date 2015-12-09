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
from collections import deque
from carbon.conf import settings

try:
    from collections import defaultdict
except ImportError:
    from util import defaultdict


class _MetricFlushQueue(object):
    def __init__(self, cache):
        self.cache = cache
        self.seen_metrics = set()
        self.queue = deque()
        super(_MetricFlushQueue, self).__init__()

    def next(self):
      '''Get the next item. There's a trick. This queue repopulates itself
      by getting a sorted list of keys from the cache.
      '''
      try:
        # usually, just return the next item in the queue
        return self.queue.pop()
      except IndexError, ie:
        # handle the rare "queue empty" event by repopulating the queue
        t = time.time()
        items = [item[0] for item in sorted(self.cache.counts, key=lambda x: x[1])]
        if settings.LOG_CACHE_QUEUE_SORTS:
          log.msg("Sorted %d cache queues in %.6f seconds" % (len(items), time.time() - t))
        self.queue.extend(items)
        return self.queue.pop()

    def prepend(self, metric):
      '''Prepend a new metric on the front of the flush queue.
      
      The new metric won't have any data, but this will get the whisper file
      on disk so that the metric is visible to graphite. Otherwise, the
      most recently added metrics don't show up for quite a while.
      '''
      if not metric in self.seen_metrics:
        self.queue.append(metric)
        self.seen_metrics.add(metric)


class _MetricCache(dict):
  def __init__(self, defaultfactory=deque, method="sorted"):
    self.size = 0
    self.method = method
    self.defaultfactory = defaultfactory
    if self.method in ("sorted", "newest_sorted"):
      self.queue = _MetricFlushQueue(self)
    else:
      self.queue = False
    super(_MetricCache, self).__init__()

  def store(self, metric, datapoint):
    self.size += 1
    try:
      self[metric].append(datapoint)
    except KeyError, ke:
      self[metric] = self.defaultfactory()
      self[metric].append(datapoint)
      if self.method == "newest_sorted":
        # notice a new metric, put it at the front of the flush queue
        self.queue.prepend(metric)

    if self.isFull():
      log.msg("MetricCache is full: self.size=%d" % self.size)
      events.cacheFull()

  def isFull(self):
    # Short circuit this test if there is no max cache size, then we don't need
    # to do the someone expensive work of calculating the current size.
    return settings.MAX_CACHE_SIZE != float('inf') and self.size >= settings.MAX_CACHE_SIZE

  def pop(self, metric=None):
    if not self:
      raise KeyError(metric)
    elif metric:
      datapoints = (metric, super(_MetricCache, self).pop(metric))
    elif not metric and self.method == "max":
      # TODO: [jssjr 2015-04-24] This is O(n^2) and suuuuuper slow.
      metric = max(self.items(), key=lambda x: len(x[1]))[0]
      datapoints = (metric, super(_MetricCache, self).pop(metric))
    elif not metric and self.method == "naive":
      datapoints = self.popitem()
    elif not metric and self.method in ("sorted", "newest_sorted"):
      metric = self.queue.next()
      # Save only last value for each timestamp
      popped = super(_MetricCache, self).pop(metric)
      ordered = sorted(dict(popped).items(), key=lambda x: x[0])
      datapoints = (metric, deque(ordered))
      # Adjust size counter if we've dropped multiple identical timestamps
      dropped = len(popped) - len(datapoints[1])
      if dropped > 0:
        self.size -= dropped
    self.size -= len(datapoints[1])
    return datapoints

  @property
  def counts(self):
    return [(metric, len(datapoints)) for (metric, datapoints) in self.items()]


# Ghetto singleton

MetricCache = _MetricCache(method=settings.CACHE_WRITE_STRATEGY)


# Avoid import circularities
from carbon import log, state, events
