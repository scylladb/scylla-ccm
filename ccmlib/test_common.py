import os
import subprocess
import unittest

from ccmlib.common import java_version_property_regexp, get_java_home_path


class Test(unittest.TestCase):
    java_19_output = """Property settings:
        file.encoding = UTF-8
        file.separator = /
        java.class.path = 
        java.class.version = 63.0
        java.home = /usr/lib/jvm/java-19-openjdk-amd64
        java.io.tmpdir = /tmp
        java.library.path = /usr/lib/x86_64-linux-gnu/jni/

            /usr/java/packages/lib
            /usr/lib/x86_64-linux-gnu/jni
            /lib/x86_64-linux-gnu
            /usr/lib/x86_64-linux-gnu
            /usr/lib/jni
            /lib
            /usr/lib
        java.runtime.name = OpenJDK Runtime Environment
        java.runtime.version = 19.0.2+7-Ubuntu-0ubuntu322.04
        java.specification.name = Java Platform API Specification
        java.specification.vendor = Oracle Corporation
        java.specification.version = 19
        java.vendor = Private Build
        java.vendor.url = Unknown
        java.vendor.url.bug = Unknown
        java.version = 19.0.2
        java.version.date = 2023-01-17
        java.vm.compressedOopsMode = Zero based
        java.vm.info = mixed mode, sharing
        java.vm.name = OpenJDK 64-Bit Server VM
        java.vm.specification.name = Java Virtual Machine Specification
        java.vm.specification.vendor = Oracle Corporation
        java.vm.specification.version = 19
        java.vm.vendor = Private Build
        java.vm.version = 19.0.2+7-Ubuntu-0ubuntu322.04
        jdk.debug = release
        line.separator = \n 
        native.encoding = UTF-8
        os.arch = amd64
        os.name = Linux
        os.version = 6.8.0-40-generic
        path.separator = :
        stderr.encoding = UTF-8
        stdout.encoding = UTF-8
        sun.arch.data.model = 64
        sun.boot.library.path = /usr/lib/jvm/java-19-openjdk-amd64/lib
        sun.cpu.endian = little
        sun.io.unicode.encoding = UnicodeLittle
        sun.java.launcher = SUN_STANDARD
        sun.jnu.encoding = UTF-8
        sun.management.compiler = HotSpot 64-Bit Tiered Compilers
        user.country = US
        user.dir = /extra/scylladb/scylla-ccm
        user.home = /home/dmitry.kropachev
        user.language = en
        user.name = dmitry.kropachev

    openjdk version "19.0.2" 2023-01-17
    OpenJDK Runtime Environment (build 19.0.2+7-Ubuntu-0ubuntu322.04)
    OpenJDK 64-Bit Server VM (build 19.0.2+7-Ubuntu-0ubuntu322.04, mixed mode, sharing)
    (.venv) ➜  scylla-ccm git:(dk/fix-jvm-lookup-logic) 
            """

    def test_java_version_property_regexp(self):
        match = java_version_property_regexp.search(self.java_19_output)
        assert match is not None
        assert len(match.groups()) == 1
        assert match.groups()[0] == '19'

    def test_get_java_home_path(self):
        from unittest.mock import patch
        from pathlib import Path
        with (
            patch.object(Path, 'exists') as mock_exists,
            patch.object(Path, 'rglob') as rglob,
            patch.object(Path, 'is_file') as is_file,
            patch.object(subprocess, 'check_output') as check_output,
            patch.object(os, 'access') as access
        ):
            access.return_value = True
            mock_exists.return_value = True
            is_file.return_value = True
            rglob.return_value = [Path('/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java')]
            check_output.return_value = self.java_19_output.encode('utf-8')
            assert get_java_home_path(Path("/usr/lib/jvm/"), ['19']) == Path('/usr/lib/jvm/java-8-openjdk-amd64/jre/')
            assert get_java_home_path(Path("/usr/lib/jvm/"), ['8']) is None
