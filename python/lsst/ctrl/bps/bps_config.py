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

"""Configuration class that adds order to searching sections for value,
expands environment variables and other config variables.
"""

__all__ = ["BPS_DEFAULTS", "BPS_SEARCH_ORDER", "BpsConfig", "BpsFormatter"]


import copy
import logging
import os
import re
import string
from os.path import expandvars

from lsst.daf.butler import Config
from lsst.resources import ResourcePath
from lsst.utils import doImport

_LOG = logging.getLogger(__name__)

# Using lsst.daf.butler.Config to resolve possible includes.
BPS_DEFAULTS = Config(ResourcePath("resource://lsst.ctrl.bps/etc/bps_defaults.yaml")).toDict()

BPS_SEARCH_ORDER = ["bps_cmdline", "payload", "cluster", "pipetask", "site", "cloud", "bps_defined"]

# Need a string that won't be a valid default value
# to indicate whether default was defined for search.
# And None is a valid default value.
_NO_SEARCH_DEFAULT_VALUE = "__NO_SEARCH_DEFAULT_VALUE__"


class BpsFormatter(string.Formatter):
    """String formatter class that allows BPS config search options."""

    def get_field(self, field_name, args, kwargs):
        _, val = args[0].search(field_name, opt=args[1])
        return val, field_name

    def get_value(self, key, args, kwargs):
        _, val = args[0].search(key, opt=args[1])
        return val


class BpsConfig(Config):
    """Contains the configuration for a BPS submission.

    Parameters
    ----------
    other : `str`, `dict`, `~lsst.daf.butler.Config`, `BpsConfig`
        Path to a YAML file or a dict/Config/BpsConfig containing configuration
        to copy.
    search_order : `list` [`str`], optional
        Root section names in the order in which they should be searched.
    defaults : `str`, `dict`, `~lsst.daf.butler.Config`, optional
        Default settings that will be used to prepopulate the config.
        If the WMS service default settings are available, they will be added
        afterwards. WMS settings takes precedence over provided defaults.
    wms_service_class_fqn : `str`, optional
        Fully qualified name of the WMS service class to use to get plugin's
        specific default settings. If ``None`` (default), the WMS service
        class provided by

        1. ``other`` config,
        2. environmental variable ``BPS_WMS_SERVICE_CLASS``,
        3. default settings

        will be used instead. The list above also reflects the priorities
        if the WMS service class is defined in multiple places. For example,
        the name of service class found in ``other`` takes precedence over
        the name of the service class provided by the BPS_SERVICE_CLASS and/or
        the default settings.

    Raises
    ------
    ValueError
        Raised if the class cannot be instantiated from the provided object.
    """

    def __init__(self, other, search_order=None, defaults=None, wms_service_class_fqn=None):
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
            other_config = Config(other)
        except Exception as exc:
            raise ValueError(f"A BpsConfig could not be loaded from other: {other}") from exc

        config = Config()

        # Pre-populate the config with default settings if any were provided
        # by the caller. Include WMS plugin specific defaults and/or
        # overrides as well if available.
        if defaults:
            config.update(defaults)

            # If the WMS service class was not specified explicitly by the
            # caller, try to use the value provided by either:
            #
            #     1. 'other' config,
            #     2. environmental variable BPS_WMS_SERVICE_CLASS,
            #     3. default settings
            #
            # (in decreasing priority).
            if wms_service_class_fqn is None:
                wms_service_class_fqn = other_config.get(
                    "wmsServiceClass",
                    os.environ.get("BPS_WMS_SERVICE_CLASS", config.get("wmsServiceClass")),
                )
            try:
                wms_service_class = doImport(wms_service_class_fqn)
            except TypeError:
                # Do not die if the WMS service class is still not set.
                pass
            else:
                wms_service = wms_service_class({})
                wms_defaults = wms_service.defaults
                if wms_defaults:
                    config.update(wms_defaults)

                # Set the service class to the one which was defaults was used.
                config["wmsServiceClass"] = wms_service_class_fqn

        # Include values and/or apply overrides from 'other' config.
        config.update(other_config)
        self.update(config)

        if isinstance(other, BpsConfig):
            self.formatter = copy.deepcopy(other.formatter)
            self.search_order = copy.deepcopy(other.search_order) if search_order is None else search_order
        else:
            self.formatter = BpsFormatter()
            self.search_order = BPS_SEARCH_ORDER if search_order is None else search_order

        # Make sure search sections exist.
        for key in self.search_order:
            if not Config.__contains__(self, key):
                self[key] = {}

    def copy(self):
        """Make a copy of config.

        Returns
        -------
        copy : `lsst.ctrl.bps.BpsConfig`
            A duplicate of itself.
        """
        return BpsConfig(self)

    def get(self, key, default=""):
        """Return the value for key if key is in the config, else default.

        If default is not given, it defaults to an empty string.

        Parameters
        ----------
        key : `str`
            Key to look for in config.
        default : Any, optional
            Default value to return if the key is not in the config.

        Returns
        -------
        val : Any
            Value from config if found, default otherwise.

        Notes
        -----
        The provided default value (an empty string) was chosen to maintain
        the internal consistency with other methods of the class.
        """
        _, val = self.search(key, opt={"default": default})
        return val

    def __getitem__(self, name):
        """Return the value from the config for the given name.

        Parameters
        ----------
        name : `str`
            Key to look for in config

        Returns
        -------
        val : `str`, `int`, `lsst.ctrl.bps.BpsConfig`, ...
            Value from config if found.
        """
        _, val = self.search(name, {})

        return val

    def __contains__(self, name):
        """Check whether name is in config.

        Parameters
        ----------
        name : `str`
            Key to look for in config.

        Returns
        -------
        found : `bool`
            Whether name was in config or not.
        """
        found, _ = self.search(name, {})
        return found

    def search(self, key, opt=None):
        """Search for key using given opt following hierarchy rules.

        Search hierarchy rules: current values, a given search object, and
        search order of config sections.

        Parameters
        ----------
        key : `str`
            Key to look for in config.
        opt : `dict` [`str`, `Any`], optional
            Options dictionary to use while searching.  All are optional.

            ``"curvals"``
                    Means to pass in values for search order key
                    (curr_<sectname>) or variable replacements.
                    (`dict`, optional)
            ``"default"``
                    Value to return if not found. (`Any`, optional)
            ``"replaceEnvVars"``
                    If search result is string, whether to replace environment
                    variables inside it with special placeholder (<ENV:name>).
                    By default set to False. (`bool`)
            ``"expandEnvVars"``
                    If search result is string, whether to replace environment
                    variables inside it with current environment value.
                    By default set to False. (`bool`)
            ``"replaceVars"``
                    If search result is string, whether to replace variables
                    inside it. By default set to True. (`bool`)
            ``"required"``
                    If replacing variables, whether to raise exception if
                    variable is undefined. By default set to False. (`bool`)

        Returns
        -------
        found : `bool`
            Whether name was in config or not.
        value : `str`, `int`, `lsst.ctrl.bps.BpsConfig`, ...
            Value from config if found.
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

        # There's a problem with the searchobj being a BpsConfig
        # and its handling of __getitem__.  Until that part of
        # BpsConfig is rewritten, force the searchobj to a Config.
        if "searchobj" in opt:
            opt["searchobj"] = Config(opt["searchobj"])

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
            print(f"\n\nError: search for {key} failed")
            print("\tcurrent = ", self.get("current"))
            print("\topt = ", opt)
            print("\tcurvals = ", curvals)
            print("\n\n")
            raise KeyError(f"Error: Search failed {key}")

        _LOG.debug("found=%s, value=%s", found, value)

        _LOG.debug("opt=%s %s", opt, type(opt))
        if found and isinstance(value, str):
            if opt.get("expandEnvVars", True):
                _LOG.debug("before format=%s", value)
                value = re.sub(r"<ENV:([^>]+)>", r"$\1", value)
                value = expandvars(value)
            elif opt.get("replaceEnvVars", False):
                value = re.sub(r"\${([^}]+)}", r"<ENV:\1>", value)
                value = re.sub(r"\$(\S+)", r"<ENV:\1>", value)

            if opt.get("replaceVars", True):
                # default only applies to original search key
                # Instead of doing deep copies of opt (especially with
                # the recursive calls), temporarily remove default value
                # and put it back.
                default = opt.pop("default", _NO_SEARCH_DEFAULT_VALUE)

                # Temporarily replace any env vars so formatter doesn't try to
                # replace them.
                value = re.sub(r"\${([^}]+)}", r"<BPSTMP:\1>", value)

                value = self.formatter.format(value, self, opt)

                # Replace any temporary env place holders.
                value = re.sub(r"<BPSTMP:([^>]+)>", r"${\1}", value)

                # if default was originally in opt
                if default != _NO_SEARCH_DEFAULT_VALUE:
                    opt["default"] = default

            _LOG.debug("after format=%s", value)

        if found and isinstance(value, Config):
            value = BpsConfig(value, search_order=[])

        return found, value
