import os
import sys
import fcntl
import tempfile
import subprocess
from contextlib import contextmanager
from shutil import copyfileobj, rmtree
from typing import Any, IO, Iterator
import argparse
import glob
import re
import signal
from urllib.parse import unquote, urlparse, parse_qs
from pathlib import Path

import rpm
from urpm import Pack


__version__ = "3.00"

def main():
    parser = argparse.ArgumentParser(description='Create a hdlist and associated media info from a directory of RPMs.')
    parser.add_argument('rpms_dir', nargs="?", help='directory containing *.rpm files')
    parser.add_argument('--clean', action='store_true', help='do not use incremental updates')
    parser.add_argument('--no-bad-rpm', action='store_true', help='do not fail on bad rpm')
    parser.add_argument('--no-md5sum', action='store_true', help='do not generate MD5SUM')
    parser.add_argument('--no-clean-old-rpms', action='store_true', help='do not clean old rpms. Ignored for now')
    parser.add_argument('--only-clean-old-rpms', action='store_true', help='only clean old rpms. Not implemented for now')
    parser.add_argument('--nolock', action='store_true', help='do not lock the media_info directory')
    parser.add_argument('--no-hdlist', action='store_true', help='do not generate hdlist.cz')
    parser.add_argument('--allow-empty-media', action='store_true', help='allow empty media')
    parser.add_argument('--file-deps', metavar='FILE', help='use file_deps.lst file')
    parser.add_argument('--hdlist-filter', metavar='FILTER', default=b".cz:gzip -9", help="use FILTER to compress hdlist, default: .cz:gzip -9")
    parser.add_argument('--synthesis-filter', metavar='FILTER', default=b".cz:xz -7", help="use FILTER to compress synthesis.hdlist, default: .cz:xz -7")
    parser.add_argument('--xml-info', action='store_true', help='Force to generate xml info. By default genhdlist3 will only regenerate xml info files already there in media_info')
    parser.add_argument('--xml-info-filter', metavar='FILTER', default=b".lzma:xz -7", help='use FILTER to compress XML media info, default: .lzma:xz -7')
    parser.add_argument('--versioned', action='store_true', help='generate versioned media info, default: no')
    parser.add_argument('--media-info-dir', metavar='DIR', help='directory containing media info files (default: %(rpms_dir)s/media_info)')
    parser.add_argument('-v', '--verbose', action='store_true', help='be verbose')
    parser.add_argument('--version', action='store_true', help='print version and exit')

    args = parser.parse_args()
    # not yet implemented
    incremental = False

    if args.no_clean_old_rpms:
        print("Option no-clean-old-rpms has no effect for now. It is ignored")
    if args.only_clean_old_rpms:
        print("Option only-clean-old-rpms is not implemented for now")
        sys.exit(1)
    if args.version:
        print(f"{sys.argv[0]} version {__version__}")
        sys.exit(0)

    if args.verbose:
        args.quiet = False

    if args.file_deps:
        with open(args.file_deps, 'r') as f:
            file_deps = [line.strip() for line in f]
    else:
        file_deps = []

    if args.rpms_dir == None:
        print("You must specify a directory containing *.rpm files")
        sys.exit(1)
    if not Path(args.rpms_dir).exists():
        print(f"The directory {args.rpms_dir} does not exist.")
        sys.exit(1)

    verbose = args.verbose
    rpms_dir = args.rpms_dir
            
    if args.media_info_dir is None:
        media_info_dir = Path(rpms_dir) / "media_info"
    if not media_info_dir.exists():
        # create the directory, if possible
         media_info_dir.mkdir(parents=True, exist_ok=True)
    else:
        # check that the directory is writable
        try:
            testfile = tempfile.TemporaryFile(dir=media_info_dir)
            testfile.close()
        except OSError as e:
            if e.errno == errno.EACCES:  # 13
                print(f"Directory {media_info_dir} is not writeable")
                sys.exit(1)
            raise

    rpms = [f for f in Path(rpms_dir).glob("*.rpm")]
    rpms.sort()
    rpms = [os.path.join(rpms_dir, f) for f in rpms]
    if not rpms and not args.allow_empty_media:
        print(f"no *.rpm files found in {rpms_dir}, use --allow-empty-media to proceed (or specify a valid rpms_dir)")
        sys.exit(1)

    versioned = args.versioned

    xml_info = args.xml_info or (media_info_dir / 'info.xml.lzma').exists()
    if not args.hdlist_filter.decode("utf-8").startswith('.'):
        raise Exception("hdlist_filter must start with '.' followed by an extension then ':' as separator for a filter including a compression level from -0 to -9 (e.g. .cz:gzip -9)")
    hdlist_filename = 'hdlist' + args.hdlist_filter.decode("utf-8").split(":")[0]
    synthesis_filename = f'synthesis.hdlist{args.synthesis_filter.decode("utf-8").split(":")[0]}'
    media_info_files = [hdlist_filename, synthesis_filename] if not args.no_hdlist else []
    media_info_files.extend(f'{f}.xml{args.xml_info_filter.decode("utf-8").split(":")[0]}' for f in ['info', 'files', 'changelog'] if xml_info)
    xml_media_info = ['info', 'files', 'changelog'] if xml_info else []
    # we don't provide anymore other option
    output_recommends = True

    if not args.nolock:
        lock = lock_file(Path(media_info_dir) / 'UPDATING')
        signal.signal(signal.SIGINT, lambda signum, frame: cleanup(lock, media_info_dir))
        signal.signal(signal.SIGTERM, lambda signum, frame: cleanup(lock, media_info_dir))

    # Force locale to be C
    # We don't translate anything but we would get translated package info and
    # wrongly put it in hdlists
    # https://bugs.mageia.org/show_bug.cgi?id=95
    os.environ['LC_ALL'] = 'C'

    """old_rpms_file = media_info_dir / 'old-rpms.lst'
    old_rpms = read_old_rpms_lst(old_rpms_file, args.nolock) if not args.only_clean_old_rpms else None
    if old_rpms:
        filter_out_old_rpms(rpms_dir, old_rpms, rpms)
        if not no_clean_old_rpms:
            clean_old_rpms(rpms_dir, old_rpms)
            write_old_rpms_lst(old_rpms, old_rpms_file)"""

    rpms_todo = {os.path.basename(f): None for f in rpms}


    if file_deps:
        print("--file_deps: This option is not yet managed")
        sys.exit(1)
        urpm = urpm.URPM()
        for file in file_deps:
            with open(file, 'r') as f:
                for line in f:
                    urpm.add_provide(unquote(line.strip()))
    synthesis_suffix, synthesis_filter = args.synthesis_filter.split(b":")
    xml_info_suffix, xml_info_filter = args.xml_info_filter.split(b":")

    synthesis = (media_info_dir / "tmp" / 'synthesis.hdlist').with_suffix(synthesis_suffix.decode("utf-8"))

    if not args.no_hdlist:
        # out_hdlist = gzip.open(hdlist_file.with_suffix(".tmp"), "wb", compresslevel=9)
        out_hdlist = Pack(Path(media_info_dir) /"tmp" / hdlist_filename, synthesis, args.hdlist_filter)

    out = {
        "hdlist": out_hdlist,
    }

    if Path(hdlist_filename).exists() and incremental:
        print(f"Filtering {hdlist_file} into {hdlist_file.with_suffix('.tmp')} : not ready, skipping")

    add_new_rpms_to_hdlist(rpms_todo, out, rpms_dir, xml_media_info, xml_info_suffix)

    for name in media_info_files:
        if verbose:
            print(f"moving {name}")
        src = (Path(media_info_dir) / "tmp" / name)
        dst = (Path(media_info_dir) / name)
        if src.exists():
            src.replace(dst)
        elif not dst.exists():
            if verbose:
                print(f"{src} doesn't exist, skipping")
            continue
    (Path(media_info_dir) / "tmp").rmdir()



    if not args.no_md5sum:
        md5sum_path = Path(media_info_dir) / 'MD5SUM'
        md5sum_path.unlink(missing_ok=True)
        import hashlib

    if versioned:
        if versioned != 'auto' or (Path(media_info_dir) / 'versioned-media-info').exists():
            import datetime
            import time
            version = datetime.datetime.fromtimestamp(time.time()).strftime("%Y%m%d-%H%M%S")

    if not args.no_md5sum:
            f = open(Path(media_info_dir) / 'MD5SUM', 'w')
            if verbose:
                print(f"creating MDSUM file")

    for file in media_info_files:
        name = file
        src = media_info_dir / file
        if versioned:
            # renaming file with a prefix in the form of YYYYMMDD-HHMMSS
            name = f"{version}-{file}"
            dst = media_info_dir / name
            src.replace(dst)
            if verbose:
                print(f"creating versioned media_info {dst}: {version}-{file}")
        else:
            dst = src
        # adding md5sum for each file if required
        if not args.no_md5sum:
            if os.path.exists(dst):
                with open(dst, 'rb') as h:
                    md5 = hashlib.md5()
                    while chunk := h.read(8192):
                        md5.update(chunk)
                    f.write(f"{md5.hexdigest()}  {name}\n")
            else:
                print(f"{name} doesn't exist, skipping")


@contextmanager
def lock_file(file):
    import fcntl

    lock_ex = 2
    lock_nb = 4

    print(f"locking {file}")
    with open(file, 'w') as lock:
        fcntl.flock(lock, lock_ex | lock_nb)
        yield
        fcntl.flock(lock, fcntl.LOCK_UN)
    os.remove(file)

def cleanup( media_info_dir):
    os.remove(media_info_dir / self.hdlist_filename.with_suffix('.tmp'))
    os.remove(media_info_dir / self.synthesis_filename.with_suffix('.tmp'))
        

def read_old_rpms_lst(file, nolock):
    import configparser

    config = configparser.ConfigParser()
    if not Path(file).exists():
        return None

    lock = lock_file(file + '.lock') if not nolock else None

    try:
        config.read(file)
    except configparser.DuplicateOptionError:
        print("duplicate option in", file)
        return None

    return {'lst': config, 'lock': lock}

def write_old_rpms_lst(old_rpms, file):
    import configparser

    if old_rpms['lock']:
        config = configparser.ConfigParser()
        config.read_dict({'Remove': dict(old_rpms['lst']['Remove'])})
        with open(file, 'w') as f:
            config.write(f)

        print("unlocking", file + '.lock')
        old_rpms['lock'].close()
        os.remove(file + '.lock')

def clean_old_rpms(rpms_dir, old_rpms):
    config = old_rpms['lst']
    for pkg in config['Remove']:
        src = os.path.join(rpms_dir, pkg)
        if os.path.exists(src):
            date = config['Remove'][pkg]
            if int(date) >= int(time.time()):
                print(f"[OLD-RPMS] keeping {pkg} (it is scheduled for {date})")
            else:
                print(f"[OLD-RPMS] removing rpm file {pkg} (was scheduled for {date})")
                os.remove(src)
        else:
            print(f"[OLD-RPMS] {pkg} already removed")

        if int(date) < int(time.time()):
            del config['Remove'][pkg]

def filter_out_old_rpms(rpms_dir, old_rpms, rpms_list):
    config = old_rpms['lst']
    keep = set(config['Remove'])
    keep.update(config['Keep-in-hdlist'])

    for pkg in rpms_list:
        if pkg not in keep:
            rpms_list.remove(pkg)
    

# Functions for reading old-rpms.lst

def _apply_date_old_rpms(rpms_dir, old_rpms, section, section_tag, do_it):
    config = old_rpms['lst']
    for pkg in config[section]:
        date = config[section][pkg]
        if os.path.exists(os.path.join(rpms_dir, pkg)):
            if int(date) >= int(time.time()):
                print(f"[{section_tag}] keeping {pkg} (it is scheduled for {date})")
            else:
                do_it(pkg, date)
        else:
            print(f"[{section_tag}] {pkg} already removed")



def filter_existing_hdlist(rpms_todo, in_hdlist, out, sizes):
    # if urpm.parse_hdlist(in_hdlist, packing=1, callback=lambda _, pkg: add_pkg(out, pkg, rpms_todo, sizes)):
        # ok
        pass
    # else:
    #     nb = len(urpm.depslist)
        nb = 0
        print(f"parse_hdlist has failed, keeping {nb} headers successfully parsed")

def add_new_rpms_to_hdlist(rpms_todo, out, rpms_dir, xml_media_info, xml_info_suffix):
    rpms_dir = Path(rpms_dir)

    for rpm in rpms_todo:
        rpm_path = rpms_dir / rpm
        if rpm_path.exists():
            with rpm_path.open("rb") as rpm_file:
                rpm_header = get_rpm_info(rpm_file)

                # create hdlist entry
                hdlist_entry = out["hdlist"].add_pkg(rpm_header, rpm_file)

    # write synthesis
    out["hdlist"].write_synthesis()
    for xml_info in xml_media_info:
        print(f"writing {xml_info}")
        out["hdlist"].write_xml(xml_info, xml_info_suffix)

    # write hdlist
    out["hdlist"].write()

def get_rpm_info(rpm_file):
    # Initialize the RPM transaction set
    ts = rpm.TransactionSet()

    # Open the RPM file
    # Extract the RPM package header
    hdr = ts.hdrFromFdno(rpm_file)
    return hdr

if __name__ == "__main__":
    main()
