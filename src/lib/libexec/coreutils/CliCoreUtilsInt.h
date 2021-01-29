// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _CLI_COREUTILS_INT_H_
#define _CLI_COREUTILS_INT_H_

#include <ncurses.h>
#include <unistd.h>
#include <panel.h>
#include <form.h>

#define wprintUnderline(win, ...) \
    wattron(win, A_UNDERLINE);    \
    wprintw(win, __VA_ARGS__);    \
    wattroff(win, A_UNDERLINE)

#define wprintBold(win, ...)   \
    wattron(win, A_STANDOUT);  \
    wprintw(win, __VA_ARGS__); \
    wattroff(win, A_STANDOUT)

#define printBold(...)   \
    attron(A_STANDOUT);  \
    printw(__VA_ARGS__); \
    attroff(A_STANDOUT)

#define printLine(shouldBold, ...)  \
    {                               \
        if (shouldBold) {           \
            printBold(__VA_ARGS__); \
        } else {                    \
            printw(__VA_ARGS__);    \
        }                           \
    }

struct WindowParams {
    WINDOW *winHandle;
    int width;
    int height;
};

struct FieldParams {
    FIELD *fieldHandle;
    int width;
    int height;
    int x;
    int y;
    int offscreen;
    int nbuffers;
    char *text;
};

struct FormParams {
    FORM *formHandle;
    bool formPosted;
    int width;
    int height;
    int x;
    int y;
    int topPadding;
    int leftPadding;
    int rightPadding;
    int botPadding;
};

// These numbers are chosen for aesthetic reasons. Any larger
// and there's not enough screen space to display all the columns/rows
// in the CLI.
enum { MaxOptionalColumns = 4, CliNumKeyValuePairs = 25 };

static inline void
addChrome(WINDOW *window, char *title)
{
    int height, width;

    getmaxyx(window, height, width);
    ((void) height);

    box(window, 0, 0);
    mvwaddch(window, 2, 0, ACS_LTEE);
    mvwhline(window, 2, 1, ACS_HLINE, width - 2);
    mvwaddch(window, 2, width - 1, ACS_RTEE);

    mvwprintw(window, 1, 2, "%s", title);
}

extern Status listDatasets(bool prettyPrint, const char *datasetName);

extern void listXdfs(const char *fnNamePattern, const char *categoryPattern);
extern void showXdf(const char *fnName);

#endif  // _CLI_COREUTILS_INT_H_
