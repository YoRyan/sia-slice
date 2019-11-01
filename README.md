# Sia Slice

...is a small program that maintains a mirror of a large file on the
[Sia](https://sia.tech) decentralized storage network. The envisioned use case
is storing a long-term, low-cost disk image, with periodic updates, for backups
and archives on The Cloud™.

The basic idea behind Sia Slice is to chop up a single block device (or other
large, monolithic file) into thousands of 100MiB chunks, LZMA-compress those
chunks, and then upload them to Sia. That way, the next time you sync, you only
have to upload the chunks that have changed since the last sync. Sync operations
always construct a complete and identical mirror; there is no history, and there
are no "full" or "incremental" snapshots. This minimizes both the complexity of
the program and the storage requirements on Sia.

Besides, if you need those features, you can simply use any filesystem you
choose on top of the original device or disk image. This ability is what makes
Sia Slice unique: In contrast to other synchronization programs like
[Siasync](https://github.com/tbenz9/siasync) and
[Repertory](https://bitbucket.org/blockstorage/repertory/src/master/) that
operate at the file level, Sia Sice operates at the *block level*.

![Curses screenshot](https://raw.githubusercontent.com/wiki/YoRyan/sia-slice/transfer-screen.png)

![Sia screenshot](https://raw.githubusercontent.com/wiki/YoRyan/sia-slice/sia-ui-screen.png)

Sia Slice was written for GNU/Linux systems with Python 3.7 or later. Ports to
other platforms should be possible with minimal effort.

The author uses Sia Slice weekly to mirror his Btrfs backup drive.

### Installation

```
pip install https://github.com/YoRyan/sia-slice
```

### Usage

You must provide the API password required to communicate with Sia, either
through the command line or by setting `$SIA_API_PASSWORD`. I recommend using
the environment variable:

```
export SIA_API_PASSWORD=xxxxxx
```

To copy the contents of `/dev/sdb1` to a new Sia folder at `/backupdrive`:

```
siaslice --mirror /dev/sdb1 backupdrive
```

(To sync again, just run the same command. Sia Slice will locate the previous
uploads and determine which blocks need to be re-uploaded.)

To download all Sia Slice data from `/backupdrive` and reassemble it on
`/dev/sdc1`:

```
siaslice --download /dev/sdc1 backupdrive
```

Finally, Sia Slice writes a timestamped state file that can be used to resume
any mirror or download operation in the event of a program crash, network
interruption, or other catastrophic event.

To resume a stalled mirror operation:

```
siaslice --resume siaslice-mirror-20191024-1522.dat
```

### Notes

Sia is an emerging technology, and I jumped through a lot of hoops to write this
software. For the curious, I have written a companion
[blog post](https://youngryan.com/2019/10/introducing-sia-slice-my-absurdly-cheap-block-storage-solution/).

For contributors and forkers, some tests are located in tests/.
```
pip install -e .
pip install asynctest
cd tests/
python -m unittest ...
```
