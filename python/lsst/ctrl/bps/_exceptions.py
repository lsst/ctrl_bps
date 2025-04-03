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

"""Specialized BPS exceptions."""

__all__ = ["BpsError", "BpsSubprocessError"]


class BpsError(Exception):
    """The base class all BPS exceptions should be derived from."""


class BpsSubprocessError(BpsError):
    """Exception raised when the subprocess spawned by BPS fails.

    Parameters
    ----------
    errno : `int`
        A numeric error code returned by the command executed in the spawned
        subprocess.
    strerror : `str`
        Message with the details about the error that occurred.
    """

    def __init__(self, errno: int, strerror: str) -> None:
        super().__init__(strerror)
        self.errno = errno
        self.strerror = strerror
