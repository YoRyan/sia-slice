#!/usr/bin/env python3

from time import sleep

from curses import wrapper

import siaslice as ss


if __name__ == '__main__':
    def test(stdscr):
        status1 = ss.OpStatus(transfers={2: 0.3, 0: 0.8, 1: 0.5},
                              current_index=4, last_index=20)
        ss.show_status(stdscr, status1, title='progress screen mockup #1')
        sleep(5)

        status2 = ss.OpStatus(transfers={2: 0.7, 0: 0.9, 3: 0.2, 4: 0.3},
                              current_index=5, last_index=20)
        ss.show_status(stdscr, status2, title='progress screen mockup #2')
        sleep(5)
    wrapper(test)

