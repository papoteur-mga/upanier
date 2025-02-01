#!/usr/bin/python3
import lzma
import gzip
import rpm
import os
import sys
import tempfile
from contextlib import contextmanager
from shutil import copyfileobj, rmtree
from typing import IO, Any, Iterator, List, Tuple, ByteString
from struct import unpack, pack
from pathlib import Path


class Pack:
    def __init__(self,
            archive: IO,
            synthesis_path: IO,
            filter: bytes = b"gzip",
            uncompress: bytes = b"", 
            extern: bool = False, 
            noargs: bool = False, 
            block_size: int = 400 * 1024, 
            bufsize: int = 65536, 
            quiet: bool = False, 
            debug: bool = False)-> None:
        
        self.destroyed: bool = False
        self.filename = archive
        try:
            self.filter = filter.decode("utf-8").split(":")[1].split(" ")[0]
            self.level = - int(filter.decode("utf-8").split(":")[1].split(" ")[1])
        except ValueError:
            raise Exception(f"Invalid hdlist filter {hdlist_filter}")
        if self.filter not in ["gzip", "xz"]:
            raise Exception(f"Invalid hdlist filter {self.filter}, it should be 'gzip' or 'xz'")
        self.force_extern = extern
        self.noargs = noargs
        self.block_size = block_size
        self.bufsize = bufsize

        self.files: dict = {}
        self.dir: dict = {}
        self.symlink: dict = {}
        self.coff: int = 0
        self.current_block_data: ByteString = b""
        self.current_block_files: List = []
        self.current_block_csize: int = 0
        self.current_block_coff: int = 0
        self.current_block_off: int = 0
        # self.ustream_data: Any = None
        self.toc_f_count: int = 0
        self.need_build_toc: bool = True

        self.log: Any = (lambda *args: print(*args)) if quiet else (lambda *args: print(*args, file=sys.stderr))
        self.debug: Any = (lambda *args: print(*args)) if debug else (lambda *args: None)
        self.file_path = synthesis_path
        self.synthesis = {}
        self.order = 0
        # fixed temporary directory for now
        # self.temp_dir = tempfile.mkdtemp(prefix='my_temp_dir_')
        self.temp_dir = synthesis_path.parent
        self.temp_dir.mkdir(exist_ok=True)
        self.handle: IO  = open(self.filename, 'wb')


    def read_synthesis(self):
        with lzma.open(self.file_path, 'rt') as f:
            lines = f.readlines()
        hdlist_dict = {}
        current_name = None
        current_entry = {}
        for line in lines:
            if line[0] == '@':
                words = line[1:].strip().split('@')
                if words[0] == "info":
                    current_entry["epoch"] = words[2]
                    current_entry["size"] = words[3]
                    current_entry["group"] = words[4]
                    hdlist_dict[words[1]] = current_entry
                    #current_name = words[1]
                    current_entry = {}
                else:
                    current_entry[words[0]] = words[1:]
        self.synthesis = hdlist_dict
        return hdlist_dict

    def write_synthesis(self):
        data = ''
        fields = ("requires","suggests","obsoletes","conflicts","provides","summary","filesize")
        for name, entry in self.synthesis.items():
            for field in fields:
                if field in entry.keys():
                    if isinstance(entry[field], list):
                        if len(entry[field]) > 0:
                            data += f"@{field}@{'@'.join(entry[field])}\n"
                    else:
                        data += f"@{field}@{'@'.join(str(x)  if isinstance(x, int)  else x for x in entry[field])}\n" if isinstance(entry[field], list)  else f"@{field}@{entry[field]}\n" 
            data += f"@info@{name.removesuffix('.rpm')}@{entry['epoch']}@{entry['size']}@{entry['group']}\n"
        with lzma.open(self.temp_dir / 'synthesis.hdlist.cz', 'wt') as f:
            f.write(data)

    def _write_files(self):
        data =  '<?xml version="1.0" encoding="utf-8"?>\n<media_info>'
        for name, entry in self.synthesis.items():
            data += f'<files fn="{name}">\n'
            if 'files' in entry.keys():
                for file in entry['files']:
                    data += file[0]+ "\n"
            data += '</files>\n'
        data += '</media_info>'
        return data
    

    def _write_info(self):
        data =  '<?xml version="1.0" encoding="utf-8"?>\n<media_info>'
        for name, entry in self.synthesis.items():
            if 'sourcerpm' not in entry.keys():
                print(f"Missing sourcerpm for {entry}")
                sys.exit(1)
            data += f"<info fn='{name}'\n sourcerpm='{entry['sourcerpm']}'\n url='{entry['url']}'\n license='{entry['license']}' >\n"
            data += entry["description"]           
            data += '</info>\n'
        data += '</media_info>'
        return data

    def _write_changelog(self):
        data =  '<?xml version="1.0" encoding="utf-8"?>\n<media_info>'
        for name, entry in self.synthesis.items():
            data += f"<changelogs fn='{name}'>\n"
            for i in range(0, len(entry["changelogname"])):
                data += f"<log time='{entry['changelogtime'][i]}'>\n<log_name>{entry['changelogname'][i]}</log_name>\n<log_text>{entry['changelogtext'][i]}</log_text>\n</log>\n"
            data += '</info>\n'
        data += '</media_info>'
        return data

    def write_xml(self, xml_info: str, xml_info_suffix: ByteString):
        if xml_info == 'files':
            data = self._write_files()
        if xml_info == 'info':
            data = self._write_info()
        if xml_info == 'changelog':
            data = self._write_changelog()
        with lzma.open((self.temp_dir / xml_info).with_suffix(".xml" +xml_info_suffix.decode('utf-8')), 'wt') as f:
            f.write(data)

    def file_sizes(self, rpm_list: List=[]):
        if rpm_list == []:
            return {name: data['filesize'] for (name, data) in self.synthesis.items() if 'filesize' in data.keys()}
        else:
            return {name: data['filesize'] for (name, data) in self.synthesis.items() if 'filesize' in data.keys() and name in rpm_list}
    
    def write(self):
        self.current_block_off = 0
        self.current_block_coff = 0
        for rpm in self.synthesis.keys():
            self.current_block_files.append(rpm)
            data = self.synthesis[rpm]['header']
            length = len(data)
            self.current_block_off += length
            self.current_block_data += data
            self.files[rpm] = {
                'size': length,
                'off': self.current_block_off,
                'csize': -1,
                'coff': self.current_block_coff,
            }
            if len(self.current_block_data) >= self.block_size:
                self.end_block()
        self.end_block()
        self.build_toc()
        self.handle.close()
        self.destroyed = True

    def add_pkg(self, hdr:ByteString, rpm_file: IO):
        
        # Get basic package information
        package_info = {
            'name': hdr[rpm.RPMTAG_NAME],
            'epoch': 0 if hdr[rpm.RPMTAG_EPOCH] == None else hdr[rpm.RPMTAG_EPOCH],
            'version': hdr[rpm.RPMTAG_VERSION],
            'release': hdr[rpm.RPMTAG_RELEASE],
            'architecture': hdr[rpm.RPMTAG_ARCH],
            'summary': hdr[rpm.RPMTAG_SUMMARY],
            'description': hdr[rpm.RPMTAG_DESCRIPTION],
            'group': hdr[rpm.RPMTAG_GROUP],
            'license': hdr[rpm.RPMTAG_LICENSE],
            'packager': hdr[rpm.RPMTAG_PACKAGER],
            'buildtime': hdr[rpm.RPMTAG_BUILDTIME],
            'sourcerpm': hdr[rpm.RPMTAG_SOURCERPM],
            'url': hdr[rpm.RPMTAG_URL],
            # 440 is the rpm header size (?) empirical, but works 
            'filesize': hdr[rpm.RPMTAG_LONGSIGSIZE] + 440,
            'size': hdr[rpm.RPMTAG_SIZE],
            'changelogtext': hdr[rpm.RPMTAG_CHANGELOGTEXT],
            'changelogname': hdr[rpm.RPMTAG_CHANGELOGNAME],
            'changelogtime': hdr[rpm.RPMTAG_CHANGELOGTIME],
        }

        # Get files in the RPM package
        files = hdr.fiFromHeader()
        package_info['files'] = files
        # Package dependencies (if any)
        if hdr.requires != []:
            package_info['requires'] = self.print_list_entry(
                hdr[rpm.RPMTAG_REQUIRES],
                iter(hdr[rpm.RPMTAG_REQUIREVERSION]),
                iter(hdr[rpm.RPMTAG_REQUIREFLAGS]),
            )
        # this could be claryfied. Are recommends and suggests equivalent or used simultaneously
        if hdr.recommends != []:
            package_info['suggests'] = self.print_list_entry(
                hdr[rpm.RPMTAG_RECOMMENDS],
                iter(hdr[rpm.RPMTAG_RECOMMENDVERSION]),
                iter(hdr[rpm.RPMTAG_RECOMMENDFLAGS]),
            )
        if hdr.conflicts != []:
            package_info['conflicts'] = self.print_list_entry(
                hdr[rpm.RPMTAG_CONFLICTS],
                iter(hdr[rpm.RPMTAG_CONFLICTVERSION]),
                iter(hdr[rpm.RPMTAG_CONFLICTFLAGS]),
                )
        if hdr.obsoletes != []: 
            package_info['obsoletes'] = self.print_list_entry(
                hdr[rpm.RPMTAG_OBSOLETES],
                iter(hdr[rpm.RPMTAG_OBSOLETEVERSION]),
                iter(hdr[rpm.RPMTAG_OBSOLETEFLAGS]),
                )    
        if hdr.provides != []: 
            package_info['provides'] = self.print_list_entry(
                hdr[rpm.RPMTAG_PROVIDES],
                iter(hdr[rpm.RPMTAG_PROVIDEVERSION]),
                iter(hdr[rpm.RPMTAG_PROVIDEFLAGS])
                )
        self.order += 1
        package_info['order'] = self.order
        package_info['header'] = hdr.unload()

        self.synthesis[os.path.basename(rpm_file.name)] = package_info

    def print_list_entry(self, names: List[str], versions: List[str], flags: List[int]) -> List[str]:
        reqs = []
        for name in names:
            version = next(versions)
            flag = next(flags)
            if not name.startswith('rpmlib('):
                if version != "":
                    constraint = ""
                    if (flag & rpm.RPMSENSE_LESS):
                        constraint = '<'
                    if (flag & rpm.RPMSENSE_GREATER):
                        constraint = '>'
                    if (flag & rpm.RPMSENSE_EQUAL):
                        constraint += '='
                    if ((flag & (rpm.RPMSENSE_LESS|rpm.RPMSENSE_EQUAL|rpm.RPMSENSE_GREATER)) == rpm.RPMSENSE_EQUAL):
                        constraint = '=='
                    reqs.append(f"{name}[{constraint} {version}]")
                else:
                    reqs.append(name)
        return reqs


    def __del__(self) -> None:
        if self.destroyed:
            return
        self.destroyed = True
        self.end_block()
        self.build_toc()
        if self.handle:
            self.handle.close()

    def build_toc(self) -> bool:
        if not self.need_build_toc:
            return True
        self.end_block()
        self.end_seek()
        toc_length = 0

        coff = self.coff
        toc_sizes_offsets = b""

        toc_str = b""
        for entry in self.dir:
            toc_str += entry + b"\n"
            toc_length += len(entry + "\n")
        for entry, link in self.symlink.items():
            toc_str += entry + b"\n" + link + b"\n"
            toc_length += len(entry + "\n" + link + "\n")
        for entry in sorted(self.files.keys()):
            toc_length += len(entry + "\n")
        for entry in sorted(self.files.keys()):
            coff, csize, off, size = self.files[entry].values()
            toc_str += entry.encode("utf-8") + b"\n"
            toc_sizes_offsets += pack(">4i", coff, csize, off, size)
            toc_length += len(pack(">4i", coff, csize, off, size))

        self.coff += toc_length
        toc_header = b"cz[0"
        toc_footer = b"0]cz"
        toc_str += pack(b">4s4i40s4s", toc_header, len(self.dir), len(self.symlink), len(self.files), toc_length, self.uncompress, toc_footer)
        self.handle.seek(self.coff, os.SEEK_SET)
        self.handle.write(toc_str)
        self.toc_f_count = len(self.files)
        return True


    def end_seek(self):
        seekvalue = self.coff
        r = self.handle.seek(seekvalue, os.SEEK_SET)
        return r == seekvalue

    def end_block(self):
        print(f"writing block with {len(self.current_block_data)} bytes")
        if not self.end_seek():
            return
        insize = len(self.current_block_data)
        if self.filter == "gzip":
            import io
            self.uncompress = b"gzip -d"
            # Créating objet io.BytesIO for storing data to compress
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode='w', compresslevel=self.level) as gzip_file:
                gzip_file.write(self.current_block_data)
            cdata = buf.getvalue()
        elif self.filter == "xz":
            cdata = lzma.compress(self.current_block_data, preset=self.level)
            self.uncompress = b"xz -d"
        outsize = len(cdata)
        self.handle.write(cdata)
        for fname in self.current_block_files:
            self.files[fname]["csize"] = self.current_block_csize
        self.current_block_csize += outsize
        self.coff += self.current_block_csize
        self.current_block_coff += self.current_block_csize
        self.current_block_csize = 0
        self.current_block_files = []
        self.current_block_off = 0
        self.current_block_data = b""

__version__ = "0.1.0"
