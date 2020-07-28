from lsst.daf.butler.core.config import Config
from os.path import expandvars
import logging
import copy
import sys
import string

FILENODE = 0
TASKNODE = 1
SEARCH_ORDER = ['payload', 'pipetask', 'site', 'global']

_LOG = logging.getLogger()


class BpsFormatter(string.Formatter):
    def get_field(self, field_name, args, kwargs):
        val = args[0].__getitem__(field_name, opt=args[1])
        return (val, field_name)

    def get_value(self, key, args, kwargs):
        # args = [ config ]
        # kwargs is equiv to opt
        val = args[0].__getitem__(key, opt=args[1])
        return val


class BpsConfig(Config):
    def __init__(self, other, *args, **kwargs):
        super().__init__()
        self.search_order = []
        self.formatter = BpsFormatter()

        if isinstance(other, BpsConfig):
            super().__init__(other)
            self.search_order = copy.deepcopy(other.search_order)
            self.formatter = copy.deepcopy(other.formatter)
        elif isinstance(other, Config):
            super().__init__(other)
        elif isinstance(other, str):
            self.__initFromFile(other)
        elif isinstance(other, dict) and 'configFile' in other:
            self.__initFromFile(other['configFile'])
        else:
            raise RuntimeError("A BpsConfig could not be loaded from other: %s" % other)

    def __initFromFile(self, configFile):
            mainCfg = Config(configFile)

            # job, pipetask, block, archive, site, global
            if 'includeConfigs' in mainCfg:
                for inc in [x.strip() for x in mainCfg['includeConfigs'].split(',')]:
                    _LOG.debug("Loading includeConfig %s" % inc)
                    incConfig = Config(inc)
                    self.update(incConfig)
                mainCfg.__delitem__('includeConfigs')
            else:
                _LOG.debug("Given config does not have key 'includeConfigs'")

            self.update(mainCfg)
            self.search_order = SEARCH_ORDER
            self.formatter = BpsFormatter()

    def copy(self):
        print("Called copy")
        return BpsConfig(self)

    def __getitem__(self, name, opt=None):
        _LOG.debug("GETITEM: %s, %s" % (name, opt))

        if opt is None:
            opt = {}

        found, val = self.search(name, opt)

        return val

    def __contains__(self, name, opt=None):
        found, _ = self.search(name, opt)
        return found

    def search(self, key, opt=None):
        """Searches for key using given opt following hierarchy rules.

        Hierarchy rules: current vals, search object, search order of config sections
        """
        _LOG.debug("search: initial key = '%s', opt = '%s'" % (key, opt))

        if opt is None:
            opt = {}

        found = False
        value = ''

        # start with stored current values
        curvals = None
        if Config.__contains__(self, 'current'):
            curvals = copy.deepcopy(Config.__getitem__(self, 'current'))
        else:
            curvals = {}

        # override with current values passed into function if given
        if 'curvals' in opt:
            for ckey, cval in list(opt['curvals'].items()):
                _LOG.debug("using specified curval %s = %s" % (ckey, cval))
                curvals[ckey] = cval

        _LOG.debug("curvals = %s" % curvals)

        if key in curvals:
            _LOG.debug("found %s in curvals" % (key))
            found = True
            value = curvals[key]
        elif 'searchobj' in opt and key in opt['searchobj']:
            found = True
            value = opt['searchobj'][key]
        else:
            for sect in self.search_order:
                if Config.__contains__(self, sect):
                    _LOG.debug("Searching '%s' section for key '%s'" % (sect, key))
                    searchSect = Config.__getitem__(self, sect)
                    if "curr_" + sect in curvals:
                        currkey = curvals["curr_" + sect]
                        _LOG.debug("currkey for section %s = %s" % (sect, currkey))
                        #searchSect = Config.__getitem__(searchSect, currkey)
                        if Config.__contains__(searchSect, currkey):
                            searchSect = Config.__getitem__(searchSect, currkey)

                    _LOG.debug("%s %s" % (key, searchSect))
                    if Config.__contains__(searchSect, key):
                        found = True
                        value = Config.__getitem__(searchSect, key)
                        break
                else:
                    _LOG.warning("Missing search section '%s' while searching for '%s'" % (sect, key))

            # lastly check root values
            if not found:
                _LOG.debug("Searching root section for key '%s'" % (key))
                if Config.__contains__(self, key):
                    found = True
                    value = Config.__getitem__(self, key)
                    _LOG.debug("root value='%s'" % (value))

        if not found and 'default' in opt:
            val = opt['default']
            found = True  #????

        if not found and opt.get('required', False):
            print("\n\nError: search for %s failed" % (key))
            print("\tcurrent = ", Config.__getitem__(self, 'current'))
            print("\topt = ", opt)
            print("\tcurvals = ", curvals)
            print("\n\n")
            raise KeyError("Error: Search failed (%s)" % key)

        _LOG.debug("found=%s, value=%s" % (found, value))

        _LOG.debug("opt=%s %s" % (opt, type(opt)))
        if found and isinstance(value, str) and opt.get('replaceVars', True):
            _LOG.debug("before format=%s" % (value))
            value = expandvars(value)  # must replace env vars before calling format
            value = self.formatter.format(value, self, opt)
            _LOG.debug("after format=%s" % (value))

        if found and isinstance(value, Config):
            value = BpsConfig(value)

        return found, value
