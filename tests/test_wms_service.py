# This file is part of ctrl_bps.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Tests for ``wms_service`` package."""

import unittest

from lsst.ctrl.bps.wms_service import WmsSpecificInfo


class WmsSpecificInfoTestCase(unittest.TestCase):
    """Tests for WmsSpecificInfo class."""

    def setUp(self):
        self.info = WmsSpecificInfo()

    def tearDown(self):
        pass

    def testInitialization(self):
        self.assertFalse(self.info.context)
        self.assertFalse(self.info.templates)

    def testAddingValidMessageWithDictionary(self):
        self.info.add_message("one: {one}, two: {two}", {"one": 1, "two": 2})
        self.assertEqual(self.info.templates, ["one: {one}, two: {two}"])
        self.assertEqual(self.info.context, {"one": 1, "two": 2})

    def testAddingValidMessageWithKeyVals(self):
        self.info.add_message("one: {one}, two: {two}", one=1, two=2)
        self.assertEqual(self.info.templates, ["one: {one}, two: {two}"])
        self.assertEqual(self.info.context, {"one": 1, "two": 2})

    def testAddingValidMessageMixed(self):
        self.info.add_message("one: {one}, two: {two}", {"one": 1}, two=2)
        self.assertEqual(self.info.templates, ["one: {one}, two: {two}"])
        self.assertEqual(self.info.context, {"one": 1, "two": 2})

    def testAddingInvalidMessageBadTemplate(self):
        with self.assertRaises(ValueError):
            self.info.add_message("one: {one", {"one": 1})

    def testAddingInvalidMessageBadContext(self):
        with self.assertRaisesRegex(ValueError, "^Adding template.*failed"):
            self.info.add_message("one: {one}", {"two": 2})

    def testAddingInvalidMessageContextConflicts(self):
        with self.assertRaisesRegex(ValueError, "^Adding template.*change of value detected"):
            self.info.add_message("one: {one}", {"one": 1})
            self.info.add_message("two: {one}", {"one": 2})

    def testRenderingSingleMessage(self):
        self.info.add_message("one: {one}", {"one": 1})
        self.assertEqual(str(self.info), "one: 1")

    def testRenderingMultipleMessages(self):
        self.info.add_message("one: {one}", {"one": 1})
        self.info.add_message("two: {two}", {"two": 2})
        self.assertEqual(self.info.context, {"one": 1, "two": 2})
        self.assertEqual(self.info.templates, ["one: {one}", "two: {two}"])
        self.assertEqual(str(self.info), "one: 1\ntwo: 2")
