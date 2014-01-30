"""tests for the new, mixin-based property/descriptor system"""

from __future__ import absolute_import

import unittest2

from normalize.record import Record
from normalize.property import LazyProperty
from normalize.property import Property
from normalize.property import ROProperty
from normalize.property.meta import MetaProperty


class TestProperties(unittest2.TestCase):
    """Test that the new data descriptor classes work"""
    def test_0_property(self):
        """Test that unbound Property objects can be created successfully"""
        prop = Property()
        self.assertIsNotNone(prop)
        self.assertIsInstance(prop, Property)
        self.assertIsInstance(type(prop), MetaProperty)

        roprop = Property(traits=['ro'])
        self.assertIsNotNone(roprop)
        self.assertIsInstance(roprop, ROProperty)
        self.assertIsInstance(type(prop), MetaProperty)

        name = ROProperty()
        self.assertIsNotNone(roprop)
        self.assertIsInstance(roprop, ROProperty)

    def test_1_basic(self):
        """Test that basic Properties can be defined and used"""
        class BasicRecord(Record):
            name = Property()

        br = BasicRecord()
        self.assertIsInstance(br, BasicRecord)
        self.assertRaises(AttributeError, lambda x: x.name, br)

        br = BasicRecord(name="Bromine")
        self.assertEqual(br.name, "Bromine")

    def test_2_ro(self):
        """Test Attributes which don't allow being set"""
        class TrivialRecord(Record):
            id = ROProperty()
            name = Property()

        tr = TrivialRecord(id=123)
        self.assertEqual(tr.id, 123)
        with self.assertRaises(AttributeError):
            tr.id = 124

        tr.name = "Travel Guides"
        self.assertEqual(tr.name, "Travel Guides")

    def test_3_lazy(self):
        """Test Attributes which are build-once"""
        class TrapDoorRecord(Record):
            def _shoot(self):
                projectile = self.chamber
                self.chamber = "empty"
                return projectile
            chamber = Property()
            fired = LazyProperty(default=_shoot)

        tdr = TrapDoorRecord(chamber="bolt")
        self.assertNotIn(
            "fired", tdr.__dict__, "peek into lazy object's dict"
        )
        self.assertEqual(tdr.fired, "bolt")
        self.assertEqual(tdr.chamber, "empty")
        self.assertEqual(tdr.fired, "bolt")
