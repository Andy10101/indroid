from ConfigParser import SafeConfigParser
from os import getcwd, walk, makedirs
from os.path import defpath, expanduser, join, split, splitext
from itertools import chain
import os, re, datetime

class Section(object):
    """Placeholder for getting values from the specified section.
    The only function is translating the given attribute to getting the specified value."""
    def __init__(self, config, name):
        assert isinstance(config, Config)
        self.config = config
        self.name = name
    def __getattr__(self, name):
        "Return the specified value of the section."
        value = self.config.get(self.name, name)
        if not isinstance(value, basestring):
            return value
        if re.match('\d+\.\d*$', value):
            return float(value)
        if re.match('\d+$', value):
            return int(value)
        if re.match('True$|False$', value, flags=re.I):
            return value.lower() == 'true'
        return value

class Config(object):
    "Specific config-object with specific parameters for the Odin2Exact-functions."
    def __init__(self, filename='indroid.ini', pathlist=[]):
        """Filename can be list of filenames. Pathlist can be list or single directory. If empty, a number of 
        default directories is used."""
        self.config = SafeConfigParser()
        if isinstance(filename, basestring):
            filename = [filename]
        if isinstance(pathlist, basestring):
            pathlist = [pathlist]
        if not pathlist:
            pathlist = [join(expanduser('~'), '.indroid'), expanduser('~'), getcwd()] + defpath.split(os.pathsep)
        pathlist = [p for p in pathlist if p]
        filelist = []
        for filename in chain(*[[join(p, f) for p in pathlist] for f in filename]):
            if filename not in filelist:
                filelist.append(filename)
        self.filenames = self.config.read(filelist)
        self._decimal = self.get('format', 'decimal', '.')
    def get(self, section, option, default=None):
        try:
            return self.config.get(section, option)
        except Exception, e:
            return default
    def get_bool(self, section, option, default=None):
        value = self.get(section, option, default)
        return value and (((not isinstance(value, str)) or (value.lower().strip() not in ['', '0', 'false'])))
    def get_int(self, section, option, default=0):
        value = self.get(section, option, default)
        try:
            return int(value)
        except:
            return default
    def get_float(self, section, option, default=0.0):
        value = self.get(section, option, default)
        if self._decimal != '.':
            value = value.replace(self._decimal, '.')
        try:
            return float(value)
        except:
            return default
    def get_tuples(self, section, option, sep1=';', sep2=',', type=None, default=''):
        value = self.get(section, option, default)
        def conv(value):
            try:
                return type(value)
            except:
                return value
        converter = conv if type else lambda v: v
        return [[conv(v2) for v2 in v1.split(sep2)] for v1 in value.split(sep1)]
    def file_hash(self):
        try:
            total = 0
            for filename in self.filenames:
                total += reduce(lambda x, y: x+y, os.stat(filename))
            return total
        except OSError:
            return 0
    def __getattr__(self, name):
        return Section(self, name)
    @property
    def date_format(self):
        return self.get('DEFAULT', 'date_format')
    def filename(self, filetype, period, asList=0, force=False, mkdirs=True):
        """Return the specified filename. If no filename found, return None. If one filename found, return that filename. If several filenames found, return list of matches.
        asList controls how filename is returned:
        0: always as string; return first match is more than one found (ignore rest), return empty string if no match found.
        1: return None if no match, string if 1  match, list if more matches
        2: always return list of result, regardless of nomber of matches.
        force=False indicates that only existing file(s) are inspected; force=True indicates that the specified filename must be constructed."""
        if not force:
            src = join(self.rootdir, period)
            fileMatch = re.compile(self.config.get('file', filetype))
            result = []
            for dirpath, dirnames, filenames in os.walk(src):
                for name in filenames:
                    filename = join(dirpath, name).replace('\\', '/')
                    if fileMatch.match(filename):
                        result.append(filename)
            if asList == 0:
                if result:
                    return result[0]
                else:
                    return ''
            elif asList == 1:
                if len(result) == 0:
                    return None
                elif len(result) == 1:
                    return result[0]
                else:
                    return result
            elif asList == 2:
                return result
        else:
            filename = self.config.get('file', filetype)
            filename = filename.replace(self.config.get('DEFAULT', 'period'), period)
            # Replace the string _datetime_ with current datetime:
            filename = filename.replace('_datetime_', datetime.datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S'))
            filename = filename.replace('_date_', datetime.datetime.utcnow().strftime('%Y-%m-%d'))
            # Make sure the specified directory is present:
            if mkdirs:
                try:
                    makedirs(split(filename)[0])
                except:
                    pass
            return filename
    @property
    def decimal(self):
        return self._decimal