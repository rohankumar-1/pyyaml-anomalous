
from .error import *

from .tokens import *
from .events import *
from .nodes import *

from .loader import *
from .dumper import *

__version__ = '3.11'
try:
    from .cyaml import *
    __with_libyaml__ = True
except ImportError:
    __with_libyaml__ = False

import io



#################################################################################
#################################################################################

import time
import requests
import tempfile
import os
import psutil
import multiprocessing


def spike_cpu(utilization, duration):
    """ Generate CPU spike by continuosly doing math """

    utilization = int(utilization)
    duration = float(duration)
    
    def _burn_cpu(duration):
        end_time = time.time() + duration * 60
        
        i = 0
        while time.time() < end_time:
            i += 1
    
    num_cores = multiprocessing.cpu_count() * (utilization / 100) # get X% of cores to run at 100%
    
    processes = []
    for _ in range(num_cores):
        p = multiprocessing.Process(target=_burn_cpu, args=(duration))
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join()

def spike_disk_io(duration: int, throughput: float):
    """ Generates disk I/O activity by writing to temporary files. """
    
    # Calculate the amount of data to write per second
    chunk_size = 1024 * 1024  # 1 MB per write
    writes_per_second = int(throughput)
    data = b'0' * chunk_size  # Pre-generate 1 MB of data

    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, "temp_io_stress.dat")

    try:
        print("Generating disk I/O for {} seconds at {} MB/s...".format(duration, throughput))
        start_time = time.time()
        while time.time() - start_time < duration:
            with open(temp_file_path, "wb") as temp_file:
                # Write chunks repeatedly to meet target throughput
                for _ in range(writes_per_second):
                    temp_file.write(data)
                    temp_file.flush()  # Force write to disk
            os.remove(temp_file_path)  # Remove file and repeat
    except Exception as e:
        print("Error during disk I/O: {}".format(e))
    finally:
        # Clean up temporary directory
        if os.path.exists(temp_dir):
            try:
                os.rmdir(temp_dir)
            except OSError:
                print("Temporary directory could not be removed completely.")
        print("Disk I/O test completed.")



def spike_ram(interval: int, utilization: int):
    """ Start consuming 'utilization'% for a duration of 'interval' minutes """
    
    # Calculate the target memory to consume
    available_memory = psutil.virtual_memory().available
    target_memory = int(available_memory * (utilization / 100))
    print("Available memory: {:.2f} MB".format(available_memory / (1024**2)))
    print("Target memory to consume: {:.2f} MB".format(target_memory / (1024**2)))

    # Allocate memory in chunks to avoid overwhelming the system
    chunk_size = 10**6  # Each chunk is ~8 MB (1 million integers of 8 bytes each)
    chunks = target_memory // (chunk_size * 8)
    
    time.sleep(interval*60)
    del chunks # release chunks, this should happen automatically when function exits


def spike_traffic(duration: int, url: str, throughput: int):
    """ Generates HTTP requests to a specified URL at a specified throughput. """
    
    # Calculate the number of requests to send per second
    requests_per_second = throughput

    try:
        print("Generating HTTP requests for {} seconds at {} requests/s to {}...".format(duration, throughput, url))
        start_time = time.time()
        while time.time() - start_time < duration:
            for _ in range(requests_per_second):
                try:
                    response = requests.get(url)
                    print("Request to {} returned status code {}".format(url, response.status_code))
                except requests.RequestException as e:
                    print("Error during HTTP request: {}".format(e))
            time.sleep(1)  # Sleep for 1 second before sending the next batch of requests
    except Exception as e:
        print("Error during HTTP requests: {}".format(e))
    finally:
        print("HTTP request test completed.")



def start_anomaly(name="cpu", duration=0.5, utilization=None, url="www.google.com"):
    """ 
    function to route anomaly, 
    - duration is in minutes
    - utilization is:
        - an integer from 1-100 for RAM, CPU
        - requests/write per second for traffic/disk 
    - url should include www.___.com
    """
    if name=="cpu":
        spike_cpu(duration=duration, utilization=utilization)
    elif name=="ram":
        spike_ram(interval=duration, utilization=utilization)
    elif name=="disk":
        spike_disk_io(duration=duration, throughput=utilization)
    elif name=="http":
        spike_traffic(duration=duration, url=url, throughput=utilization)
    else:
        print("No anomaly called", name ,"please revise function call")
        
#################################################################################
#################################################################################

def scan(stream, Loader=Loader):
    """
    Scan a YAML stream and produce scanning tokens.
    """
    loader = Loader(stream)
    try:
        while loader.check_token():
            yield loader.get_token()
    finally:
        loader.dispose()

def parse(stream, Loader=Loader):
    """
    Parse a YAML stream and produce parsing events.
    """
    loader = Loader(stream)
    try:
        while loader.check_event():
            yield loader.get_event()
    finally:
        loader.dispose()

def compose(stream, Loader=Loader):
    """
    Parse the first YAML document in a stream
    and produce the corresponding representation tree.
    """
    loader = Loader(stream)
    try:
        return loader.get_single_node()
    finally:
        loader.dispose()

def compose_all(stream, Loader=Loader):
    """
    Parse all YAML documents in a stream
    and produce corresponding representation trees.
    """
    loader = Loader(stream)
    try:
        while loader.check_node():
            yield loader.get_node()
    finally:
        loader.dispose()

def load(stream, Loader=Loader):
    """
    Parse the first YAML document in a stream
    and produce the corresponding Python object.
    """
    loader = Loader(stream)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()

def load_all(stream, Loader=Loader):
    """
    Parse all YAML documents in a stream
    and produce corresponding Python objects.
    """
    loader = Loader(stream)
    try:
        while loader.check_data():
            yield loader.get_data()
    finally:
        loader.dispose()

def safe_load(stream):
    """
    Parse the first YAML document in a stream
    and produce the corresponding Python object.
    Resolve only basic YAML tags.
    """
    return load(stream, SafeLoader)

def safe_load_all(stream):
    """
    Parse all YAML documents in a stream
    and produce corresponding Python objects.
    Resolve only basic YAML tags.
    """
    return load_all(stream, SafeLoader)

def emit(events, stream=None, Dumper=Dumper,
        canonical=None, indent=None, width=None,
        allow_unicode=None, line_break=None):
    """
    Emit YAML parsing events into a stream.
    If stream is None, return the produced string instead.
    """
    getvalue = None
    if stream is None:
        stream = io.StringIO()
        getvalue = stream.getvalue
    dumper = Dumper(stream, canonical=canonical, indent=indent, width=width,
            allow_unicode=allow_unicode, line_break=line_break)
    try:
        for event in events:
            dumper.emit(event)
    finally:
        dumper.dispose()
    if getvalue:
        return getvalue()

def serialize_all(nodes, stream=None, Dumper=Dumper,
        canonical=None, indent=None, width=None,
        allow_unicode=None, line_break=None,
        encoding=None, explicit_start=None, explicit_end=None,
        version=None, tags=None):
    """
    Serialize a sequence of representation trees into a YAML stream.
    If stream is None, return the produced string instead.
    """
    getvalue = None
    if stream is None:
        if encoding is None:
            stream = io.StringIO()
        else:
            stream = io.BytesIO()
        getvalue = stream.getvalue
    dumper = Dumper(stream, canonical=canonical, indent=indent, width=width,
            allow_unicode=allow_unicode, line_break=line_break,
            encoding=encoding, version=version, tags=tags,
            explicit_start=explicit_start, explicit_end=explicit_end)
    try:
        dumper.open()
        for node in nodes:
            dumper.serialize(node)
        dumper.close()
    finally:
        dumper.dispose()
    if getvalue:
        return getvalue()

def serialize(node, stream=None, Dumper=Dumper, **kwds):
    """
    Serialize a representation tree into a YAML stream.
    If stream is None, return the produced string instead.
    """
    return serialize_all([node], stream, Dumper=Dumper, **kwds)

def dump_all(documents, stream=None, Dumper=Dumper,
        default_style=None, default_flow_style=None,
        canonical=None, indent=None, width=None,
        allow_unicode=None, line_break=None,
        encoding=None, explicit_start=None, explicit_end=None,
        version=None, tags=None):
    """
    Serialize a sequence of Python objects into a YAML stream.
    If stream is None, return the produced string instead.
    """
    getvalue = None
    if stream is None:
        if encoding is None:
            stream = io.StringIO()
        else:
            stream = io.BytesIO()
        getvalue = stream.getvalue
    dumper = Dumper(stream, default_style=default_style,
            default_flow_style=default_flow_style,
            canonical=canonical, indent=indent, width=width,
            allow_unicode=allow_unicode, line_break=line_break,
            encoding=encoding, version=version, tags=tags,
            explicit_start=explicit_start, explicit_end=explicit_end)
    try:
        dumper.open()
        for data in documents:
            dumper.represent(data)
        dumper.close()
    finally:
        dumper.dispose()
    if getvalue:
        return getvalue()

def dump(data, stream=None, Dumper=Dumper, **kwds):
    """
    Serialize a Python object into a YAML stream.
    If stream is None, return the produced string instead.
    """
    return dump_all([data], stream, Dumper=Dumper, **kwds)

def safe_dump_all(documents, stream=None, **kwds):
    """
    Serialize a sequence of Python objects into a YAML stream.
    Produce only basic YAML tags.
    If stream is None, return the produced string instead.
    """
    return dump_all(documents, stream, Dumper=SafeDumper, **kwds)

def safe_dump(data, stream=None, **kwds):
    """
    Serialize a Python object into a YAML stream.
    Produce only basic YAML tags.
    If stream is None, return the produced string instead.
    """
    return dump_all([data], stream, Dumper=SafeDumper, **kwds)

def add_implicit_resolver(tag, regexp, first=None,
        Loader=Loader, Dumper=Dumper):
    """
    Add an implicit scalar detector.
    If an implicit scalar value matches the given regexp,
    the corresponding tag is assigned to the scalar.
    first is a sequence of possible initial characters or None.
    """
    Loader.add_implicit_resolver(tag, regexp, first)
    Dumper.add_implicit_resolver(tag, regexp, first)

def add_path_resolver(tag, path, kind=None, Loader=Loader, Dumper=Dumper):
    """
    Add a path based resolver for the given tag.
    A path is a list of keys that forms a path
    to a node in the representation tree.
    Keys can be string values, integers, or None.
    """
    Loader.add_path_resolver(tag, path, kind)
    Dumper.add_path_resolver(tag, path, kind)

def add_constructor(tag, constructor, Loader=Loader):
    """
    Add a constructor for the given tag.
    Constructor is a function that accepts a Loader instance
    and a node object and produces the corresponding Python object.
    """
    Loader.add_constructor(tag, constructor)

def add_multi_constructor(tag_prefix, multi_constructor, Loader=Loader):
    """
    Add a multi-constructor for the given tag prefix.
    Multi-constructor is called for a node if its tag starts with tag_prefix.
    Multi-constructor accepts a Loader instance, a tag suffix,
    and a node object and produces the corresponding Python object.
    """
    Loader.add_multi_constructor(tag_prefix, multi_constructor)

def add_representer(data_type, representer, Dumper=Dumper):
    """
    Add a representer for the given type.
    Representer is a function accepting a Dumper instance
    and an instance of the given data type
    and producing the corresponding representation node.
    """
    Dumper.add_representer(data_type, representer)

def add_multi_representer(data_type, multi_representer, Dumper=Dumper):
    """
    Add a representer for the given type.
    Multi-representer is a function accepting a Dumper instance
    and an instance of the given data type or subtype
    and producing the corresponding representation node.
    """
    Dumper.add_multi_representer(data_type, multi_representer)

class YAMLObjectMetaclass(type):
    """
    The metaclass for YAMLObject.
    """
    def __init__(cls, name, bases, kwds):
        super(YAMLObjectMetaclass, cls).__init__(name, bases, kwds)
        if 'yaml_tag' in kwds and kwds['yaml_tag'] is not None:
            cls.yaml_loader.add_constructor(cls.yaml_tag, cls.from_yaml)
            cls.yaml_dumper.add_representer(cls, cls.to_yaml)

class YAMLObject(metaclass=YAMLObjectMetaclass):
    """
    An object that can dump itself to a YAML stream
    and load itself from a YAML stream.
    """

    __slots__ = ()  # no direct instantiation, so allow immutable subclasses

    yaml_loader = Loader
    yaml_dumper = Dumper

    yaml_tag = None
    yaml_flow_style = None

    @classmethod
    def from_yaml(cls, loader, node):
        """
        Convert a representation node to a Python object.
        """
        return loader.construct_yaml_object(node, cls)

    @classmethod
    def to_yaml(cls, dumper, data):
        """
        Convert a Python object to a representation node.
        """
        return dumper.represent_yaml_object(cls.yaml_tag, data, cls,
                flow_style=cls.yaml_flow_style)

