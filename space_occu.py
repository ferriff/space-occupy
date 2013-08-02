#!/usr/bin/env python
#
# federico dot ferri at cern dot ch, 01.08.2013
#

import os
from optparse import OptionParser
import random
from string import lowercase, uppercase
import sys
import time

descr = """Fill up a fraction of the available disk space of a given mount
point with binary files of random content. The size of the files is
Gaussian-generated around a central value, with a given sigma.
The program can be daemonized to run in background.
Options are available to customize most of the program parameters.
"""

parser = OptionParser(usage = '%prog [options]', version = 'v0.0', description=descr)
parser.add_option('-c', '--count', dest = 'cnt', default = 1, help = 'start numbering files from CNT [%default]')
parser.add_option('-d', '--dir', dest = 'odir', default = '.', help = 'output directory [%default]')
parser.add_option('-D', '--daemon', dest = 'daemon', action = 'store_true', default = False, help = 'deamonize and automatically write files to fill quota [%default]')
parser.add_option('-f', '--fraction', dest = 'qfrac', default = 0.5, help = 'fill this fraction of the available space [%default]')
parser.add_option('-g', '--sigma', dest = 'gsigma', default = 0.1, help = 'sigma of the gaussian fluctuation of the output file size [%default]')
parser.add_option('-l', '--log_file', dest = 'lfile', default = sys.stdout, help = 'log file [stdout]')
parser.add_option('-o', '--output_file', dest = 'ofile', default = 'output.dat', help = 'output file [%default]')
parser.add_option('-p', '--polling_time', dest = 'ptime', default = 60, help = 'interval (seconds) between polling requests for available space in daemon mode [%default]')
parser.add_option('-q', '--quota_path', dest = 'qpath', default = ".", help = 'path to check the quota of [%default]')
parser.add_option('-s', '--size', dest = 'size', default = 1, help = 'size of output file in MBytes [%default]')

(options, args) = parser.parse_args()

size = 1024 * 1024 * int(options.size)
ofile = options.ofile
odir = options.odir
ptime = int(options.ptime)
qpath = options.qpath
qfrac = float(options.qfrac)
gsigma = float(options.gsigma)
daemon = options.daemon
cnt = int(options.cnt)

letters = list(lowercase + uppercase)

def flog(of, s):
        of.write('[%.2f] "' % time.time() + time.ctime() + '" ' + s + "\n")

def write_file(of, size):
        global cnt
        s = of.rsplit(".", 1)
        of = s[0] + "_" + str(cnt) + "_" + "".join(random.sample(letters, 4)) + "." + s[1]
        flog(lfile, "writing " + of)
        fout = open(of, 'wb')
        fout.write(os.urandom(size))
        fout.close()
        cnt += 1


def free_space(path):
        flog(lfile, "checking quota for path `" + path + "'")
        (sin, sout) = os.popen2(["df", path])
        fs = 0
        first = False
        for l in sout:
                if first:
                        fs = int(l.split()[3])
                        break
                first = True
        return fs * 1024


def fill_quota(odir, nbytes, frac):
        nfiles = int(nbytes * frac) / size
        flog(lfile, "going to write " + str(nfiles) + " file(s)")
        for i in range(nfiles):
                s = int(size * random.gauss(1, gsigma))
                nbytes -= s
                if nbytes < 0: s = nbytes
                write_file(odir + "/" + ofile, s)


def main_loop(ifork = False):
        while(1):
                pid = 1
                if ifork:
                        pid = os.fork()
                if pid == 0:
                        time.sleep(ptime)
                        main_loop()
                        sys.exit(0)
                else:
                        freebytes = free_space(qpath)
                        fill_quota(odir, freebytes, qfrac)
                        sys.exit(0)


if daemon:
        pid = os.fork()
        if pid == 0:
                flog(lfile, "daemonizing...")
                flog(lfile, "polling time: " + str(ptime) + " second(s)")
                main_loop()
        else:
                sys.exit(0)
else:
        main_loop()
