all : empty.img 40MiBempty.img 20Kstripe.img 40Mstripe.img

empty.img :
	touch $@

40MiBempty.img :
	dd if=/dev/zero of=$@ bs=1000000 count=40
