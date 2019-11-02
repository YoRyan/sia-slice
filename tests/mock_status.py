#!/usr/bin/env python3

from time import sleep

from curses import wrapper

import siaslice as ss


if __name__ == '__main__':
    bs = 100*1000*1000
    status1 = ss.OpStatus(transfers={2: 0.3, 0: 0.8, 1: 0.5},
                          current_index=4, last_index=20, block_size=bs)
    title1 = 'progress screen mockup #1'
    status2 = ss.OpStatus(transfers={2: 0.7, 0: 0.9, 3: 0.2, 4: 0.3},
                          current_index=5, last_index=20, block_size=bs)
    title2 = 'progress screen mockup #2'

    def curses(stdscr):
        ss.show_status(stdscr, status1, title1)
        sleep(5)
        ss.show_status(stdscr, status2, title2)
        sleep(5)
    wrapper(curses)

    def text():
        ss.show_status(None, status1, title1)
        ss.show_status(None, status2, title2)
    text()

