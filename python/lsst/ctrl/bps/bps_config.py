# This file is part of ctrl_bps.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

"""
Configuration class that adds order to searching sections for value,
expands environment variables and other config variables
"""

from os.path import expandvars
import logging
import copy
import string

from lsst.daf.butler.core.config import Config

_LOG = logging.getLogger(__name__)


class BpsFormatter(string.Formatter):
    """String formatter class that allows BPS config
    search options
    """
    def get_field(self, field_name, args, kwargs):
        _, val = args[0].search(field_name, opt=args[1])
        return (val, field_name)

    def get_value(self, key, args, kwargs):
        _, val = args[0].search(key, opt=args[1])
        return val


class BpsConfig(Config):
    """Contains the configuration for a BPS submission.

    Parameters
    ----------
    other : `str`, `dict`, `Config`, `BpsConfig`
        Path to a yaml file or a dict/Config/BpsConfig containing configuration
        to copy.
    search_order : `list` of `str`, optional
        Root section names in the order in which they should be searched.
    """
    def __init__(self, other, search_order=None):
        # In BPS config, the same setting can be defined multiple times in
        # different sections.  The sections are search in a pre-defined
        # order. Hence, a value which is found first effectively overrides
        # values in later sections, if any. To achieve this goal,
        # the special methods __getitem__ and __contains__ were redefined to
        # use a custom search function internally.  For this reason we can't
        # use  super().__init__(other) as the super class defines its own
        # __getitem__ which is utilized during the initialization process (
        # e.g. in expressions like self[<key>]). However, this function will
        # be overridden by the one defined here, in the subclass.  Instead
        # we just initialize internal data structures and populate them
        # using the inherited update() method which does not rely on super
        # class __getitem__ method.
        super().__init__()
        try:
            config = Config(other)
        except RuntimeError:
            raise RuntimeError("A BpsConfig could not be loaded from other: %s" % other)
        self.update(config)

        if isinstance(other, BpsConfig):
            self.search_order = copy.deepcopy(other.search_order)
            self.formatter = copy.deepcopy(other.formatter)
        else:
            if search_order is None:
                search_order = []
            self.search_order = search_order
            self.formatter = BpsFormatter()

        # Make sure search sections exist
        for key in self.search_order:
            if not Config.__contains__(self, key):
                self[key] = {}

    def copy(self):
        """Makes a copy of config

        Returns
        -------
        copy : `~lsst.ctrl.bps.bps_config.BpsConfig`
            A duplicate of itself
        """
        return BpsConfig(self)

    def __getitem__(self, name):
        """Returns the value from the config for the given name

        Parameters
        ----------
        name : `str`
            Key to look for in config

        Returns
        -------
        val : `str`, `int`, `~lsst.ctrl.bps.bps_config.BPSConfig`, ...
            Value from config if found
        """
        _, val = self.search(name, {})

        return val

    def __contains__(self, name):
        """Checks whether name is in config.

        Parameters
        ----------
        name : `str`
            Key to look for in config.

        Returns
        -------
        found : `bool`
            Whether name was in config or not
        """
        found, _ = self.search(name, {})
        return found

    def search(self, key, opt=None):
        """Searches for key using given opt following hierarchy rules.

        Search hierarchy rules: current values, a given search object, and
        search order of config sections.

        Parameters
        ----------
        key : `str`
            Key to look for in config.
        opt : `dict`, optional
            Options dictionary to use while searching.  All are optional.

            ``"curvals"``
                    Means to pass in values for search order key
                    (curr_<sectname>) or variable replacements.
                    (`dict`, optional)
            ``"default"``
                    Value to return if not found. (`Any`, optional)
            ``"replaceVars"``
                    If search result is string, whether to replace variables
                    inside it. By default set to True. (`bool`)
            ``"required"``
                    If replacing variables, whether to raise exception if
                    variable is undefined. By default set to False. (`bool`)

        Returns
        -------
        found : `bool`
            Whether name was in config or not
        value : `str`, `int`, `BpsConfig`, ...
            Value from config if found
        """
        _LOG.debug("search: initial key = '%s', opt = '%s'", key, opt)

        if opt is None:
            opt = {}

        found = False
        value = ""

        # start with stored current values
        curvals = None
        if Config.__contains__(self, "current"):
            curvals = copy.deepcopy(Config.__getitem__(self, "current"))
        else:
            curvals = {}

        # override with current values passed into function if given
        if "curvals" in opt:
            for ckey, cval in list(opt["curvals"].items()):
                _LOG.debug("using specified curval %s = %s", ckey, cval)
                curvals[ckey] = cval

        _LOG.debug("curvals = %s", curvals)

        if key in curvals:
            _LOG.debug("found %s in curvals", key)
            found = True
            value = curvals[key]
        elif "searchobj" in opt and key in opt["searchobj"]:
            found = True
            value = opt["searchobj"][key]
        else:
            for sect in self.search_order:
                if Config.__contains__(self, sect):
                    _LOG.debug("Searching '%s' section for key '%s'", sect, key)
                    search_sect = Config.__getitem__(self, sect)
                    if "curr_" + sect in curvals:
                        currkey = curvals["curr_" + sect]
                        _LOG.debug("currkey for section %s = %s", sect, currkey)
                        # search_sect = Config.__getitem__(search_sect, currkey)
                        if Config.__contains__(search_sect, currkey):
                            search_sect = Config.__getitem__(search_sect, currkey)

                    _LOG.debug("%s %s", key, search_sect)
                    if Config.__contains__(search_sect, key):
                        found = True
                        value = Config.__getitem__(search_sect, key)
                        break
                else:
                    _LOG.debug("Missing search section '%s' while searching for '%s'", sect, key)

            # lastly check root values
            if not found:
                _LOG.debug("Searching root section for key '%s'", key)
                if Config.__contains__(self, key):
                    found = True
                    value = Config.__getitem__(self, key)
                    _LOG.debug("root value='%s'", value)

        if not found and "default" in opt:
            value = opt["default"]
            found = True  # ????

        if not found and opt.get("required", False):
            print("\n\nError: search for %s failed" % (key))
            print("\tcurrent = ", self.get("current"))
            print("\topt = ", opt)
            print("\tcurvals = ", curvals)
            print("\n\n")
            raise KeyError("Error: Search failed (%s)" % key)

        _LOG.debug("found=%s, value=%s", found, value)

        _LOG.debug("opt=%s %s", opt, type(opt))
        if found and isinstance(value, str) and opt.get("replaceVars", True):
            _LOG.debug("before format=%s", value)
            value = expandvars(value)  # must replace env vars before calling format
            value = self.formatter.format(value, self, opt)
            _LOG.debug("after format=%s", value)

        if found and isinstance(value, Config):
            value = BpsConfig(value)

        return found, value
