#!/usr/bin/env python3

from random import randint, random
from time import sleep

from curses import wrapper

import siaslice as ss


if __name__ == '__main__':
    def mockup(last):
        return ss.OpStatus(transfers=dict((i, random()) for i in range(last)),
                           current_index=randint(0, last), last_index=last,
                           block_size=100*1000*1000)
    mockups = [mockup(5), mockup(10), mockup(50)]

    def curses(stdscr):
        for i, status in enumerate(mockups):
            ss.show_status(stdscr, status, f'progress screen #{i + 1}')
            sleep(2)
    wrapper(curses)

    def text():
        for i, status in enumerate(mockups):
            ss.show_status(None, status, f'progress screen #{i + 1}')
    text()

