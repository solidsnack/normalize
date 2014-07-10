#
# This file is a part of the normalize python library
#
# normalize is free software: you can redistribute it and/or modify
# it under the terms of the MIT License.
#
# normalize is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# MIT License for more details.
#
# You should have received a copy of the MIT license along with
# normalize.  If not, refer to the upstream repository at
# http://github.com/hearsaycorp/normalize
#

from __future__ import absolute_import

import types

from normalize.coll import Collection
import normalize.exc as exc
from normalize.record import Record
from normalize.selector import FieldSelector


class Visitor(object):
    """Class for writing Record visitor pattern functions.  This Visitor is
    intended to be subclassed; a generic version which can take the apply and
    reduce functions as arguments may be implemented later.
    """
    def __init__(self, ignore_none=True, ignore_empty_string=False,
                 apply_records=False, apply_empty_slots=False):
        """Create a new visitor.  The built-in parameters, which affect the
        default 'map' function operation, are:

        args:

            ``ignore_none=``\ *bool*
                If the 'apply' function returns ``None``, treat it as if the
                slot or object did not exist.  ``True`` by default.

            ``ignore_empty_string=``\ *bool*
                If the 'apply' function returns the empty string, treat it as
                if the slot or object did not exist.  ``False`` by default.

            ``apply_records=``\ *bool*
                Normally, traversal happens in depth-first order, and fields
                which are Records never have ``apply`` called on them.  If you
                want them to, set this field.  This affects the arguments
                passed to ``reduce_record``.

                If the ``apply`` function returns ``self.StopVisiting`` (or an
                instance of it), then traversal does not descend into the
                fields of the record.  If it returns something else, then
                ``reduce_record`` is passed a tuple as its first argument.
        """
        self.ignore_none = ignore_none
        self.ignore_empty_string = ignore_empty_string
        self.apply_records = apply_records
        self.apply_empty_slots = apply_empty_slots

    def apply(self, value, fs, prop=None, parent_obj=None):
        """'apply' is a general place to put a function which is called
        on every extant record slot.

        Data structures are normally applied in depth-first order.  If you
        specify ``apply_records`` to the constructor, this function is called
        on the actual records first, and may prevent recursion by returning
        ``self.StopVisiting(return_value)``.  Any value (other than ``None``)
        returned by applying to a record changes the value passed to
        ``reduce_record``.

        The default implementation passes through the slot value as-is, but
        expected exceptions are converted to ``None``.

        args:

            ``value=``\ *value*\ \|\ *AttributeError*\ \|\ *KeyError*
                This is the value currently in the slot, or the Record itself
                with ``apply_records``.  *AttributeError* will only be received
                if you passed ``apply_empty_slots``, and *KeyError* will be
                passed if ``parent_obj`` is a ``dict`` (see
                :py:meth:`Visitor.map_prop` for details about when this might
                happen)

            ``fs=``\ *FieldSelector*
                This is the location of ``value``, relative to where the
                visiting started.

            ``prop=``\ *Property*\ \|\ ``None``
                This is the :py:class:`normalize.Property` instance which
                represents the field being traversed.  ``None`` with
                ``apply_records``

            ``parent_obj=``\ *Record*\ \|\ ``None``
                This is the instance which the value exists in.
                ``prop.get(parent_obj)`` will return ``value`` (or throw
                ``AttributeError``). ``None`` with ``apply_records``
        """
        return (
            None if isinstance(value, (AttributeError, KeyError)) else
            value
        )

    def reduce_record(self, applied, fs, record_type):
        """Hook called for each record, with the results of mapping each
        member.

        The default implementation returns the first argument as-is.

        args:

            ``applied=``\ *dict*\ \|\ *tuple*

                This is the result of mapping the individual slots in the
                record as a dict.  The keys are the attribute names, and the
                values the result from their ``apply`` call.  With
                ``ignore_none`` (the default), this dictionary will be missing
                those keys.  Similarly with ``ignore_empty_string`` and empty
                string applied results.

                With ``apply_records``, if the first call to ``apply`` on the
                Record returned anything other than ``this.StopVisiting(x)`` or
                ``None``, the value will be a tuple, with the first item the
                result of the first call to ``apply`` on the Record, and the
                second item the result of applying the individual slots.  If
                you did return ``this.StopVisiting(x)``, then ``applied`` will
                be ``x``, whatever that was.

            ``fs=``\ *FieldSelector*
                This is the location of the record being reduced, relative to
                the starting point.

            ``record_type=``\ *RecordType*
                This is the :py:class:`normalize.Record` *class* which
                is currently being reduced.
        """
        return applied

    def reduce_collection(self, result_coll_generator, fs, coll_type):
        """Hook called for each normalize.coll.Collection.

        The default implementation calls
        :py:meth:`normalize.coll.Collection.tuples_to_coll` with
        ``coerce=False``, which just re-assembles the collection into a native
        python collection type of the same type of the input collection.

        args:

            ``result_coll_generator=`` *generator func*
                Generator which returns (key, value) pairs (like
                :py:meth:`normalize.coll.Collection.itertuples`)

            ``fs=``\ *FieldSelector*
                This is the location of the collection, relative to the
                starting point.

            ``coll_type=``\ *CollectionType*
                This is the :py:class:`normalize.coll.Collection`-derived
                *class* which is currently being reduced.
        """
        return coll_type.tuples_to_coll(result_coll_generator, coerce=False)

    def reduce_complex(self, record_result, coll_result, fs, value_type):
        """If a Collection also has properties that map to something (which you
        can only do by sub-classing ``RecordList`` or another
        :py:class:`normalize.coll.Collection` and adding properties), this
        reduction is called to combine the two applied/reduced values into a
        single value for return.

        The default implementation throws ``coll_result`` into
        ``record_result['values']``, and throws an exception if it was already
        present.

        args:

            ``record_result=``\ *dict*
                This contains whatever ``reduce_record`` returned, which will
                generally be a dictionary.

            ``coll_result=``\ *list\*\ \|\ *set*\ \|\ *etc*
                This contains whatever ``reduce_collection`` returned, normally
                a list.

            ``fs=``\ *FieldSelector*
                Location of the collection being reduced.

            ``value_type=``\ *CollectionType*
                This is the :py:class:`normalize.coll.Collection`-derived
                *class* which is currently being reduced.  Remember that
                ``Collection`` is a ``Record`` sub-class, so it has
                ``properties`` and all those other fields available.

        """
        if record_result.get("values", False):
            raise exc.VisitorTooSimple(
                fs=fs,
                value_type_name=value_type.__name__,
                visitor=type(self).__name__,
            )
        record_result['values'] = coll_result
        return record_result

    class StopVisiting(object):
        return_value = None

        def __init__(self, return_value):
            self.return_value = return_value

    def map(self, value, fs=None, value_type=None):
        """This is the 'front door' to the Visitor, and is called recursively.
        It calls the various ``map_``\ *X* methods on its argument, and the
        ``reduce_``\ *X* methods to collate the result.

        args:

            ``value=``\ *object*
                The value to visit.  Normally (but not always) a
                :py:class:`normalize.record.Record` instance.

            ``fs=``\ *FieldSelector*
                This is used to pass down the current selector context to
                ``apply`` functions, etc.  In case they need it.  I'm not sure
                what for, either.  Posterity and debug messages, if nothing
                else.

            ``value_type=``\ *RecordType*
                This is the ``Record`` subclass to interpret ``value`` as.  The
                default is ``type(value)``.  If you specify this, then the type
                information on ``value`` is essentially ignored (with the
                caveat mentioned below on :py:meth:`Visitor.map_prop`, and may
                be a ``dict``, ``list``, etc.
        """
        if not fs:
            fs = FieldSelector([])
        if not value_type:
            value_type = type(value)

        prune = False

        if issubclass(value_type, Record):
            record_mapped = self.map_record(value, fs, value_type)

            if record_mapped == self.StopVisiting or isinstance(
                record_mapped, self.StopVisiting
            ):
                record_mapped = record_mapped.return_value
                prune = True

        if not prune and issubclass(value_type, Collection):
            coll_mapped = self.reduce_collection(
                self.map_collection(value, fs, value_type), fs, value_type,
            )

            if coll_mapped and record_mapped:
                return self.reduce_complex(
                    record_mapped, coll_mapped, fs, value_type,
                )
            elif coll_mapped:
                return coll_mapped

        return record_mapped

    def map_record(self, record, fs, record_type):
        """Method responsible for calling apply on each of the fields of the
        object, and returning the value passed to
        :py:meth:`Visitor.reduce_record`.

        The default implementation first calls :py:meth:`Visitor.apply` on the
        record (with ``apply_records``), skipping recursion if that method
        returns ``self.StopVisiting`` (or an instance thereof)

        It then iterates over the properties (of the type, not the instance) in
        more-or-less random order, calling into :py:meth:`Visitor.map_prop` for
        each, the results of which end up being the dictionary return value.

        args:

            ``record=``\ *Record*
                The instance being iterated over

            ``fs=``\ *FieldSelector*
                The current path in iteration.

            ``record_type=``\ *RecordType*
                The :py:class:`normalize.record.Record` sub-class which applies
                to the instance.
        """
        if not record_type:
            record_type = type(record)
        if not fs:
            fs = FieldSelector([])

        if self.apply_records:
            result = self.apply(record, fs, None, None)
            if result == self.StopVisiting or \
                    isinstance(result, self.StopVisiting):
                return result.return_value

        result_dict = dict()

        for name, prop in record_type.properties.iteritems():
            mapped = self.map_prop(record, prop, fs)
            if mapped is None and self.ignore_none:
                pass
            elif mapped == "" and self.ignore_empty_string:
                pass
            else:
                result_dict[name] = mapped

        to_reduce = (
            result_dict if not self.apply_records or result is None else
            (result, result_dict)
        )

        return self.reduce_record(to_reduce, fs, record_type)

    def map_prop(self, record, prop, fs):
        """Method responsible for retrieving a value from the slot of an
        object, and calling :py:meth:`Visitor.apply` on it.  With
        ``apply_empty_slots``, this value will be an ``AttributeError``.

        The default implementation of this will happily handle non-Record
        objects which respond to the same accessor fetches.  Otherwise, if the
        ``record`` is a dictionary, it will pull out the named key and call
        ``Visitor.apply()`` on that instead.  With ``apply_empty_slots``, the
        value will be a ``KeyError``.

        This function will recurse back into :py:meth:`Visitor.map` if the type
        on the *property* indicates that the value contains a record.
        **caveat**: if no type hint is passed, then the type of the *value*
        will be used to determine whether or not to recurse.  This switching
        from visiting types back to visiting values will not occur if the
        structure being walked does not have Record objects, and nor will it
        occur if you specify the types of the columns being visited.

        args:

            ``record=``\ *Record*
                The instance being iterated over

            ``fs=``\ *FieldSelector*
                The current path in iteration.

            ``record_type=``\ *RecordType*
                The :py:class:`normalize.record.Record` sub-class which applies
                to the instance.
        """
        try:
            value = prop.__get__(record)
        except AttributeError, e:
            if isinstance(record, Record):
                value = e
            else:
                try:
                    value = record[prop.name]
                except TypeError:
                    value = e
                except KeyError, ae:
                    value = ae

        mapped = None
        if self.apply_empty_slots or not isinstance(
            value, (KeyError, AttributeError, types.NoneType)
        ):
            fs = fs + [prop.name]
            value_type = prop.valuetype or type(value)
            if isinstance(value_type, tuple):
                mapped = self.map_type_union(
                    value, fs, value_type, prop, record,
                )
            elif issubclass(value_type, Record):
                mapped = self.map(value, fs, value_type)
            else:
                mapped = self.apply(value, fs, prop, record)

        return mapped

    def map_collection(self, coll, fs, coll_type):
        """Generator method responsible for iterating over items in a
        collection, and recursively calling :py:meth:`Visitor.map()` on them to
        build a result.  Yields the tuple protocol: always (K, V).

        The default implementation calls the ``.itertuples()`` method of the
        value, falling back to ``coll_type.coll_to_tuples(coll)`` (see
        :py:meth:`normalize.coll.Collection.coll_to_tuples`).  On each item, it
        recurses to ``self.map()``.
        """
        try:
            generator = coll.itertuples()
        except AttributeError:
            generator = coll_type.coll_to_tuples(coll)

        for key, value in generator:
            mapped = self.map(value, fs + [key], coll_type.itemtype)
            if mapped is None and self.ignore_none:
                pass
            elif mapped == "" and self.ignore_empty_string:
                pass
            else:
                yield key, mapped

    def map_type_union(self, value, fs, type_tuple, *apply_args):
        """This corner-case method applies when visiting a value and
        encountering a type union in the ``Property.valuetype`` field.

        It first checks to see if the value happens to be an instance of one or
        more of the types in the union, and then calls :py:meth:`Visitor.map`
        on those, in order, until one of them returns something.  If none of
        these return anything, the last one called is the applied result.

        If the value is not an instance of any of them, then it tries again,
        this time calling ``map()`` with the type of each of the ``Record``
        types in the union, in turn.  If any of them return a non-empty dict,
        then that is returned.  If none of these attempts return anything (or
        there are no ``Record`` sub-classes in the type union), then
        :py:meth:`Visitor.apply` is called on the slot instead.
        """
        # this code has the same problem that record_id does;
        # that is, it doesn't know which of the type union the
        # value is.
        record_types = []
        matching_record_types = []

        for value_type in type_tuple:
            if issubclass(value_type, Record):
                record_types.append(value_type)
            if isinstance(value, value_type):
                matching_record_types.append(value_type)

        mapped = None
        if matching_record_types:
            for value_type in matching_record_types:
                mapped = self.map(value, fs, value_type)
                if mapped:
                    break
        else:
            for value_type in record_types:
                mapped = self.map(value, fs, value_type)
                if mapped:
                    break

            if not mapped:
                mapped = self.apply(value, fs, *apply_args)

        return mapped
