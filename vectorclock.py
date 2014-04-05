#!/usr/bin/env python
'''
        Vector clock class
    By  Ted Kirkpatrick
    Extension of original version by David Drysdale, included in:
    https://github.com/daviddrysdale/pynamo
    Documentation at http://lurklurk.org/pynamo/pynamo.html

    License: Version 2 of GPL: http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
'''

import copy

# PART coreclass
class VectorClock(object):
    def __init__(self):
        self.clock = {}  # node => counter

    def update(self, node, counter):
        """Add a new node:counter value to a VectorClock."""
        if counter < 0:
            raise Exception("Node %s assigned negative count %d" % (node, counter))
        if node in self.clock and counter <= self.clock[node]:
            raise Exception("Node %s has gone backwards from %d to %d" %
                            (node, self.clock[node], counter))
        self.clock[node] = counter
        return self  # allow chaining of .update() operations

    @classmethod
    def fromDict(cls, dct):
        """ Create a VectorClock from a dictionary. """
        vc = VectorClock()
        for node, count in dct.iteritems():
            vc.update(node, count)
        return vc

    def asDict(self):
        return self.clock

    def isValidClock(self):
        """ Return True if this is a valid clock. """
        for node, count in self.clock.iteritems():
            if not isinstance(node, (str, unicode)) or not isinstance(count, int) or count < 0:
                return False
        return True

    def __str__(self):
        return "{%s}" % ", ".join(["%s:%d" % (node, self.clock[node])
                                   for node in sorted(self.clock.keys())])

    def __repr__(self):
        """ Represent the clock in JSON style, with the keys in double quotes. """
        return "{%s}" % ", ".join(["\"%s\":%d" % (node, self.clock[node])
                                   for node in sorted(self.clock.keys())])

# PART comparisons
    # Comparison operations. Vector clocks are partially ordered, but not totally ordered.
    def __eq__(self, other):
        return self.clock == other.clock

    def __lt__(self, other):
        if self == other:
            return False
        for node in self.clock:
            if node not in other.clock:
                return False
            elif self.clock[node] > other.clock[node]:
                return False
        return True

    def __ne__(self, other):
        return not (self == other)

    def __le__(self, other):
        return (self == other) or (self < other)

    def __gt__(self, other):
        return (other < self)

    def __ge__(self, other):
        return (self == other) or (self > other)

# PART converge
    @classmethod
    def converge(cls, vcs):
        """Return a single VectorClock that subsumes all of the input VectorClocks"""
        result = cls()
        for vc in vcs:
            if vc is None:
                continue
            for node, counter in vc.clock.items():
                if node in result.clock:
                    if result.clock[node] < counter:
                        result.clock[node] = counter
                else:
                    result.clock[node] = counter
        return result

# -----------IGNOREBEYOND: test code ---------------
import unittest


class VectorClockTestCase(unittest.TestCase):
    """Test vector clock class"""

    def setUp(self):
        self.c1 = VectorClock()
        self.c1.update('A', 1)
        self.c2 = VectorClock()
        self.c2.update('B', 2)

    def testSmall(self):
        self.assertEquals(str(self.c1), "{A:1}")
        self.c1.update('A', 2)
        self.assertEquals(str(self.c1), "{A:2}")
        self.c1.update('A', 200)
        self.assertEquals(str(self.c1), "{A:200}")
        self.c1.update('B', 1)
        self.assertEquals(str(self.c1), "{A:200, B:1}")

    def testInternalError(self):
        self.assertRaises(Exception, self.c2.update, 'B', 1)

    def testEquality(self):
        self.assertEquals(self.c1 == self.c2, False)
        self.assertEquals(self.c1 != self.c2, True)
        self.c1.update('B', 2)
        self.c2.update('A', 1)
        self.assertEquals(self.c1 == self.c2, True)
        self.assertEquals(self.c1 != self.c2, False)

    def testOrder(self):
        self.assertEquals(self.c1 < self.c2, False)
        self.assertEquals(self.c2 < self.c1, False)
        self.assertEquals(self.c1 <= self.c2, False)
        self.assertEquals(self.c2 <= self.c1, False)
        self.c1.update('B', 2)
        self.assertEquals(self.c1 < self.c2, False)
        self.assertEquals(self.c2 < self.c1, True)
        self.assertEquals(self.c1 <= self.c2, False)
        self.assertEquals(self.c2 <= self.c1, True)
        self.assertEquals(self.c1 > self.c2, True)
        self.assertEquals(self.c2 > self.c1, False)
        self.assertEquals(self.c1 >= self.c2, True)
        self.assertEquals(self.c2 >= self.c1, False)

    def testCoalesce(self):
        self.c1.update('B', 2)
        self.assertEquals(VectorClock.coalesce((self.c1, self.c1, self.c1)), [self.c1])
        c3 = copy.deepcopy(self.c1)
        c4 = copy.deepcopy(self.c1)
        # Diverge the two clocks
        c3.update('X', 200)
        c4.update('Y', 100)
        # c1 < c3, c1 < c4
        self.assertEquals(VectorClock.coalesce(((self.c1, c3, c4))), [c3, c4])
        self.assertEquals(VectorClock.coalesce((c3, self.c1, c3, c4)), [c3, c4])

    def testConverge(self):
        self.c1.update('B', 1)
        c3 = copy.deepcopy(self.c1)
        c4 = copy.deepcopy(self.c1)
        # Diverge two of the clocks
        c3.update('X', 200)
        self.c1.update('Y', 100)
        cx = VectorClock.converge((self.c1, self.c2, c3, c4))
        self.assertEquals(str(cx), "{A:1, B:2, X:200, Y:100}")
        cy = VectorClock.converge(VectorClock.coalesce((self.c1, self.c2, c3, c4)))
        self.assertEquals(str(cy), "{A:1, B:2, X:200, Y:100}")


if __name__ == "__main__":
    unittest.main()
