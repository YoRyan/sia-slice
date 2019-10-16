all : empty.img 40MiB.img 20Kstripe.img 40Mstripe.img

empty.img :
	touch $@

40MiB.img :
	dd if=/dev/zero of=$@ bs=1000000 count=40

20Kstripe.img :
	dd if=/dev/zero of=$@ bs=20K count=4
	dd if=/dev/urandom of=$@ bs=20K count=1 seek=1
	dd if=/dev/urandom of=$@ bs=20K count=1 seek=3

40Mstripe.img :
	dd if=/dev/zero of=$@ bs=40M count=4
	dd if=/dev/urandom of=$@ bs=40M count=1 seek=1
	dd if=/dev/urandom of=$@ bs=40M count=1 seek=3
