
import unittest2

from normalize import Record
from normalize import Property
from normalize.property.types import StringProperty
import normalize.avro as avro


class TestAvroVisitor(unittest2.TestCase):
    def test_avro(self):
        class SimpleRecord(Record):
            avro_name = "test"
            a = Property(isa=long)
            b = StringProperty()

        self.assertEqual(
            avro.AvroVisitor.reflect(SimpleRecord),
            # example from http://avro.apache.org/docs/1.7.6/spec.html#schema_complex
            {
                "type": "record", 
                "name": "test",
                "fields" : [
                    {"name": "a", "type": "long"},
                    {"name": "b", "type": "string"}
                ]
            }
        )
