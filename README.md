# Sia Slice

...is a program that maintains a mirror of a large file on the
[Sia](https://sia.tech) decentralized storage network. The envisioned use case
is storing a long-term, low-cost disk image (with periodic updates) for backups
and archives on The Cloud™.

The basic idea behind Sia Slice is to chop up a single block device (or other
large, monolithic file) into thousands of 100MiB chunks, LZMA-compress those
chunks, and then upload them to Sia. That way, the next time you sync, you only
have to upload the chunks that have changed since the last sync. Sync operations
always construct a complete and identical mirror; there is no history, and there
are no "full" or "incremental" snapshots. This minimizes both the complexity of
the program and the storage requirements on Sia.

Besides, if you need those features, you can simply use any filesystem you
choose on top of the original device or disk image. This ability is what
distinguishes Sia Slice: in contrast to other synchronization programs like
[Siasync](https://github.com/tbenz9/siasync) that operate at the file level, Sia
Sice operates at the *block level*.

![Curses screenshot](https://raw.githubusercontent.com/wiki/YoRyan/sia-slice/transfer-screen.png)

Sia Slice was written for GNU/Linux. Ports to other platforms should be possible
with minimal effort.

The author uses Sia Slice weekly to mirror his Btrfs backup drive.

### Installation

```
pip install https://github.com/YoRyan/nuxhash
```

### Usage

Coming soon.

### Notes

Sia is an emerging technology, and I jumped through a lot of hoops to write this
software. For the curious, I (will soon write) a companion blog post.

For contributors and forkers, some tests are located in tests/.
```
pip install -e .
pip install asynctest
cd tests/
python -m unittest ...
```
