// Copyright 2015 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <cstdlib>
#include <stdio.h>
#include <sys/stat.h>
#include <dirent.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"
#include "coreutils/CliCoreUtilsInt.h"
#include "util/MemTrack.h"
#include "OrderingEnums.h"
#include "dag/DagLib.h"
#include "strings/String.h"
#include "GetOpt.h"
#include "util/Archive.h"
#include "dag/RetinaTypes.h"

typedef void (*MainFn)(const char *moduleName, int argc, char *argv[],
                       bool prettyPrint, bool interactive);
typedef void (*UsageFn)(const char *moduleName, int argc, char *argv[]);

// make
static void retinaMake(const char *moduleName, int argc, char *argv[],
                       bool prettyPrint, bool interactive);
static void retinaMakeHelp(const char *moduleName, int argc, char *argv[]);

// list
static void retinaList(const char *moduleName, int argc, char *argv[],
                       bool prettyPrint, bool interactive);
static void retinaListHelp(const char *moduleName, int argc, char *argv[]);

// delete
static void retinaDelete(const char *moduleName, int argc, char *argv[],
                          bool prettyPrint, bool interactive);
static void retinaDeleteHelp(const char *moduleName, int argc, char *argv[]);

// update
static void retinaUpdate(const char *moduleName, int argc, char *argv[],
                         bool prettyPrint, bool interactive);
static void retinaUpdateHelp(const char *moduleName, int argc, char *argv[]);

// listParameters
static void retinaListParams(const char *moduleName, int argc, char *argv[],
                             bool prettyPrint, bool interactive);
static void retinaListParamsHelp(const char *moduleName, int argc,
                                 char *argv[]);

// load
static void retinaLoad(const char *moduleName, int argc, char *argv[],
                       bool prettyPrint, bool interactive);
static void retinaLoadHelp(const char *moduleName, int argc, char *argv[]);

// save
static void retinaSave(const char *moduleName, int argc, char *argv[],
                       bool prettyPrint, bool interactive);
static void retinaSaveHelp(const char *moduleName, int argc, char *argv[]);

struct SubCmdMappings {
    const char *subCmdString;
    const char *subCmdDesc;
    MainFn main;
    UsageFn usageFn;
};

const SubCmdMappings subCmdMappings[] =
{
    { .subCmdString = "make",
      .subCmdDesc = "Create a new retina",
      .main = retinaMake,
      .usageFn = retinaMakeHelp,
    },
    { .subCmdString = "list",
      .subCmdDesc = "List retinas",
      .main = retinaList,
      .usageFn = retinaListHelp,
    },
    { .subCmdString = "delete",
      .subCmdDesc = "delete a particular retina",
      .main = retinaDelete,
      .usageFn = retinaDeleteHelp,
    },
    { .subCmdString = "update",
      .subCmdDesc = "Modify a retina",
      .main = retinaUpdate,
      .usageFn = retinaUpdateHelp,
    },
    { .subCmdString = "listParameters",
      .subCmdDesc = "List all parameters used in retina",
      .main = retinaListParams,
      .usageFn = retinaListParamsHelp,
    },
    { .subCmdString = "load",
      .subCmdDesc = "Loads a retina from a retina file",
      .main = retinaLoad,
      .usageFn = retinaLoadHelp,
    },
    { .subCmdString = "save",
      .subCmdDesc = "Saves a retina onto a retina file",
      .main = retinaSave,
      .usageFn = retinaSaveHelp,
    },
};

void
cliRetinaHelp(int argc, char *argv[])
{
    unsigned ii;
    if (argc > 1) {
        for (ii = 0; ii < ArrayLen(subCmdMappings); ii++) {
            if (strcmp(argv[1], subCmdMappings[ii].subCmdString) == 0) {
                if (subCmdMappings[ii].usageFn != NULL) {
                    subCmdMappings[ii].usageFn(argv[0], argc - 1, &argv[1]);
                } else {
                    printf("No help found for %s\n",
                           subCmdMappings[ii].subCmdString);
                }
                return;
            }
        }
    }

    printf("Usage: %s <retina-sub-command>\n", argv[0]);
    printf("Possible <retina-sub-command> are:\n");
    for (ii = 0; ii < ArrayLen(subCmdMappings); ii++) {
        printf("  %s - %s\n", subCmdMappings[ii].subCmdString,
               subCmdMappings[ii].subCmdDesc);
    }
}

void
cliRetinaMain(int argc, char *argv[], XcalarWorkItem *workItemIn,
              bool prettyPrint, bool interactive)
{
    unsigned ii;

    assert(workItemIn == NULL);

    if (argc == 1) {
        cliRetinaHelp(argc, argv);
        return;
    }

    assert(argc > 1);

    for (ii = 0; ii < ArrayLen(subCmdMappings); ii++) {
        if (strcmp(argv[1], subCmdMappings[ii].subCmdString) == 0) {
            assert(subCmdMappings[ii].main != NULL);
            subCmdMappings[ii].main(argv[0], argc - 1, &argv[1],
                                    prettyPrint, interactive);
            return;
        }
    }

    fprintf(stderr, "Error: No such command %s\n", argv[1]);
    cliRetinaHelp(argc, argv);
}

void
cliExecRetinaHelp(int argc, char *argv[])
{
    printf("Usage: %s --retinaName <retinaName> --dstTable <newTableName>"
           " --parameters <argName0:argValue0&argName1:argValue1&...>\n",
           argv[0]);
}



void
cliExecRetinaMain(int argc, char *argv[], XcalarWorkItem *workItemIn,
                  bool prettyPrint, bool interactive)
{
    Status status = StatusUnknown;
    Status *statusOutput;
    XcalarWorkItem *workItem = workItemIn;

    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort, cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    statusOutput = &workItem->output->hdr.status;
    if (*statusOutput == StatusOk) {
        printf("Retina \"%s\" executed successfully\n",
               workItem->input->executeRetinaInput.retinaName);
    } else {
        fprintf(stderr, "Error. Server returned: %s\n",
                strGetFromStatus(*statusOutput));
    }

    assert(status == StatusOk);
CommonExit:
    if (status == StatusCliParseError) {
        cliExecRetinaHelp(argc, argv);
    } else if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

static Status
pickTableNames(char (*tableNameArray)[DagTypes::MaxNameLen + 1],
               uint64_t *numTablesOut)
{
    int ret;
    int y, x;
    XcalarWorkItem *workItem;
    const char *defaultPattern = "*";
    XcalarApiListDagNodesOutput *listNodesOutput = NULL;
    Status status = StatusUnknown;
    uint64_t numTablesChosen = 0;
    bool confirm = false;

    initscr();
    refresh();
    cbreak();
    keypad(stdscr, TRUE);
    noecho();
    curs_set(0);

    workItem = xcalarApiMakeListDagNodesWorkItem(defaultPattern, SrcTable);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    assert(workItem != NULL);
    workItem->legacyClient = true;

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort, cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    status = workItem->output->hdr.status;
    if (status != StatusOk) {
        goto CommonExit;
    }

    listNodesOutput = &workItem->output->outputResult.listNodesOutput;

    // Start of interactive display screen
    confirm = false;
    do {
        uint64_t startRowNum, currRowNum, endRowNum, numRowsToShow, totalRows;
        uint64_t ii;
        int ch;
        int tableYOffset = 8;

        totalRows = listNodesOutput->numNodes;
        bool tableSelected[totalRows];
        for (ii = 0; ii < totalRows; ii++) {
            tableSelected[ii] = false;
        }

        startRowNum = currRowNum = 0;
        getmaxyx(stdscr, y, x);
        numRowsToShow = y - tableYOffset;
        endRowNum = xcMin(startRowNum + numRowsToShow, totalRows);

        bool exitPickTable = false;
        do {
            clear();
            printw("Creating Retina\n\n");
            printw("Select the tables you would like to use as template.\n"
                   "Press SPACE to select or unselect a table. "
                   "Press ENTER to move to the next step.\n\n");
            printw("%-40s%-20s\n", "Table Name", "Selected");
            printw("==========================================================="
                   "=========\n");
            for (ii = startRowNum; ii < endRowNum; ii++) {
                printLine((ii == currRowNum), "%-40s%-20s\n",
                          listNodesOutput->nodeInfo[ii].name,
                          (tableSelected[ii] ? "true" : "false"));
            }

            ch = getch();

            switch (ch) {
            case 'q':
                goto CommonExit;
            case '\n':
            case KEY_ENTER:
                exitPickTable = true;
                break;
            case ' ':
                tableSelected[currRowNum] = !tableSelected[currRowNum];
                break;
            case KEY_UP:
                if (currRowNum > 0) {
                    currRowNum--;
                    if (currRowNum < startRowNum) {
                        startRowNum--;
                    }
                } else {
                    startRowNum = totalRows - numRowsToShow;
                    currRowNum = totalRows - 1;
                }
                break;
            case KEY_DOWN:
                if (currRowNum < (totalRows - 1)) {
                    currRowNum++;
                    if (currRowNum >= endRowNum) {
                        endRowNum++;
                        startRowNum++;
                    }
                } else {
                    currRowNum = startRowNum = 0;
                }
                break;
            }

            getmaxyx(stdscr, y, x);
            numRowsToShow = y - tableYOffset;
            endRowNum = xcMin(startRowNum + numRowsToShow, totalRows);
        } while (!exitPickTable);

        numTablesChosen = 0;
        assert(ArrayLen(tableSelected) == listNodesOutput->numNodes);
        for (ii = 0; ii < listNodesOutput->numNodes; ii++) {
            if (tableSelected[ii]) {
                uint64_t idx = numTablesChosen++;
                strlcpy(tableNameArray[idx], listNodesOutput->nodeInfo[ii].name,
                        sizeof(tableNameArray[idx]));
            }
        }

        bool exitConfirmPage = false;
        confirm = (numTablesChosen > 0);
        do {
            clear();
            if (numTablesChosen == 0) {
                printw("You did not choose any tables!\n");
            } else {
                printw("You have chosen the following table(s):\n\n");
                for (ii = 0; ii < numTablesChosen; ii++) {
                    printw("%lu. %s\n", ii + 1, tableNameArray[ii]);
                }
            }

            printw("\nIs this correct?  ");
            printLine(confirm, "  Yes  ");
            printLine(!confirm, "  No  ");

            ch = getch();
            switch (ch) {
            case KEY_LEFT:
            case KEY_RIGHT:
                confirm = !confirm;
                break;
            case '\n':
            case KEY_ENTER:
                exitConfirmPage = true;
                break;
            case 'q':
                numTablesChosen = 0;
                goto CommonExit;
            }
        } while (!exitConfirmPage);

    } while (!confirm);

CommonExit:
    ret = endwin();
    assert(ret == OK);

    *numTablesOut = numTablesChosen;

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        listNodesOutput = NULL;
    }

    return StatusOk;
}

enum { DagNodeWidth = 28, DagNodeHeight = 5, DagNodePadding = 2 };
struct DagNodeSprite {
    int x;
    int y;
    XcalarApiDagNode *dagNode;
    struct DagNodeSprite *child;
    struct DagNodeSprite *parent;
    struct DagNodeSprite *prevSibling;
    struct DagNodeSprite *nextSibling;
};

static void
positionDagNodesInt(DagNodeSprite *childNode, int currXIn, int currYIn,
                    int *newYOut)
{
    int currX = currXIn;
    int currY = currYIn;
    int newY = currY + DagNodeHeight;

    DagNodeSprite *parentNode;

    parentNode = childNode->parent;

    if (parentNode != NULL) {
        do {
            positionDagNodesInt(parentNode, currX + DagNodeWidth, currY, &newY);
            currY = newY;
            parentNode = parentNode->nextSibling;
        } while (parentNode != childNode->parent && parentNode != NULL);
    }

    if (newY > (currYIn + DagNodeHeight)) {
        currY = (newY + currYIn - DagNodeHeight) / 2;
    } else {
        currY = currYIn;
    }

    childNode->x = currX;
    childNode->y = currY;
    *newYOut = newY;
}

static void
positionDagNodeSprites(DagNodeSprite *rootNode)
{
    int currX;
    int currY, newY;

    DagNodeSprite *currNode;

    currX = currY = 0;

    currNode = rootNode;

    do {
        positionDagNodesInt(currNode, currX, currY, &newY);
        currNode = currNode->nextSibling;
        currY = newY;
    } while (currNode != rootNode);
}

static void
unserializeDagNodesInt(XcalarApiDagOutput *serialDag,
                       DagNodeSprite unserialDag[], uint64_t *currNodeIdxOut,
                       uint64_t numNodes)
{
    int nodesToProcess = 1;

    DagNodeSprite *headNode, *prevNode, *currNode, *childNode;
    uint64_t currNodeIdx, parentNodeIdx;
    int numParents;

    currNodeIdx = *currNodeIdxOut;

    while (nodesToProcess > 0 && currNodeIdx < numNodes) {
        childNode = &unserialDag[currNodeIdx];

        currNodeIdx++;
        nodesToProcess--;

        // Can't infer this from API. This needs to be a field in
        // XcalarApiDagNode
        numParents = childNode->dagNode->numParents;
        if (numParents > 0) {
            headNode = &unserialDag[currNodeIdx + nodesToProcess];
            prevNode = headNode;
            childNode->parent = headNode;
            for (parentNodeIdx = currNodeIdx + nodesToProcess;
                 parentNodeIdx < currNodeIdx + nodesToProcess + numParents;
                 parentNodeIdx++) {
                currNode = &unserialDag[parentNodeIdx];
                currNode->child = childNode;
                currNode->dagNode = serialDag->node[parentNodeIdx];

                if (currNode != prevNode) {
                    currNode->nextSibling = prevNode->nextSibling;
                    prevNode->nextSibling = currNode;
                    currNode->prevSibling = prevNode;
                    headNode->prevSibling = currNode;
                }
                prevNode = currNode;
            }
        }

        nodesToProcess += numParents;
    }

    *currNodeIdxOut = currNodeIdx;
}

static void
unserializeDagNodes(XcalarApiDagOutput *serialDag, DagNodeSprite unserialDag[])
{
    uint64_t currNodeIdx;

    DagNodeSprite *headNode, *currNode, *prevNode;

    headNode = &unserialDag[0];
    prevNode = headNode;

    currNodeIdx = 0;
    while (currNodeIdx < serialDag->numNodes) {
        currNode = &unserialDag[currNodeIdx];
        currNode->dagNode = serialDag->node[currNodeIdx];
        unserializeDagNodesInt(serialDag, unserialDag, &currNodeIdx,
                               serialDag->numNodes);

        if (currNode != prevNode) {
            currNode->nextSibling = prevNode->nextSibling;
            prevNode->nextSibling = currNode;
            currNode->prevSibling = prevNode;
            headNode->prevSibling = currNode;
        }

        prevNode = currNode;
    }


}

static Status
drawDag(char *retinaCanvas, int canvasHeight, int canvasWidth,
        DagNodeSprite dagNodeSprites[], XcalarApiDagNode *dagNodes[],
        uint64_t numNodes)
{
    int x, y;
    uint64_t ii, jj;

    Status status = StatusUnknown;

    for (ii = 0; ii < numNodes; ii++) {
        // Check width fits in canvas
        if (dagNodeSprites[ii].x < 0 ||
            dagNodeSprites[ii].x + DagNodeWidth > canvasWidth) {
            status = StatusCliCanvasTooSmall;
            goto CommonExit;
        }

        // Check height fits in canvas
        if (dagNodeSprites[ii].y < 0 ||
            dagNodeSprites[ii].y + DagNodeWidth > canvasHeight) {
            status = StatusCliCanvasTooSmall;
            goto CommonExit;
        }

        x = dagNodeSprites[ii].x;
        y = dagNodeSprites[ii].y;

        assert(DagNodeHeight == 5);
        for (jj = 1; jj < DagNodeWidth - 2 - DagNodePadding; jj++) {
            retinaCanvas[((y + 1) * canvasWidth) + jj + x + DagNodePadding]
                                                                          = '-';
            retinaCanvas[((y + 3) * canvasWidth) + jj + x + DagNodePadding]
                                                                          = '-';
        }

        int textWidth = DagNodeWidth - 4 - (2 * DagNodePadding);
        const char *text = strGetFromXcalarApis(dagNodes[ii]->hdr.api);
        int textLen = (int) strlen(text);

        retinaCanvas[((y + 2) * canvasWidth) + x + DagNodePadding] = '|';
        retinaCanvas[((y + 2) * canvasWidth) + (DagNodeWidth - 1) + x
                                                        - DagNodePadding] = '|';

        strlcpy(&retinaCanvas[((y + 2) * canvasWidth) + x + DagNodePadding + 2 +
                              xcMax(0, ((textWidth - textLen) / 2))],
                text, textWidth);
    }
    status = StatusOk;
CommonExit:
    return status;
}

static void
displayCanvas(char *canvas, int canvasHeight, int canvasWidth, int startX,
              int startY, int windowHeight, int windowWidth,
              DagNodeSprite *selectedNode)
{
    int x, y;

    for (y = startY; y < xcMin(canvasHeight, startY + windowHeight); y++) {
        for (x = startX; x < xcMin(canvasWidth, startX + windowWidth); x++) {
            if (y == selectedNode->y + 2 &&
                x >= selectedNode->x + 1 + DagNodePadding &&
                x <= selectedNode->x + DagNodeWidth - DagNodePadding - 2) {
                attron(A_STANDOUT);
            } else {
                attroff(A_STANDOUT);
            }

            if (canvas[(y * canvasWidth) + x] != '\0') {
                printw("%c", canvas[(y * canvasWidth) + x]);
            } else {
                printw(" ", canvas[(y * canvasWidth) + x]);
            }
        }
        printw("\n");
    }
}

static inline bool
isParameterizable(XcalarApis api)
{
    return api == XcalarApiBulkLoad;
}

static bool
showParamNodeWindow(DagNodeSprite *selectedNode,
                    XcalarApiParamInput *paramInput)
{
    bool updateNode = false;
    WindowParams mainWindow = {
        .winHandle = NULL,
        .width = 0,
        .height = 0,
    };
    PANEL *panel = NULL;
    int y, x;
    Status status;
    unsigned ii;
    bool exit = false;

    FormParams mainForm = {
        .formHandle = NULL,
        .formPosted = false,
        .width = 0,
        .height = 0,
        .x = 0,
        .y = 0,
        .topPadding = 3,
        .leftPadding = 3,
        .rightPadding = 3,
        .botPadding = 3,
    };

    FieldParams instructions = {
        .fieldHandle = NULL,
        .width = 0,
        .height = 1,
        .x = 0,
        .y = 0,
        .offscreen = 0,
        .nbuffers = 0,
        .text = (char *)"Press ENTER to save. Press ESC to cancel.",
    };
    instructions.width = (int) strlen(instructions.text) + 1;

    FieldParams label = {
        .fieldHandle = NULL,
        .width = 0,
        .height = 1,
        .x = 0,
        .y = 2,
        .offscreen = 0,
        .nbuffers = 0,
        .text = (char *)"Dataset URL",
    };
    label.width = (int) strlen(label.text) + 1;

    FieldParams textInputField = {
        .fieldHandle = NULL,
        .width = 50,
        .height = 1,
        .x = 0,
        .y = 2,
        .offscreen = 0,
        .nbuffers = 0,
        .text = (char *)"",
    };
    char textInputBuf[XcalarApiMaxFileNameLen + 1];

    bool fieldMissing = false;

    instructions.fieldHandle = new_field(instructions.height,
                                         instructions.width,
                                         instructions.y, instructions.x,
                                         instructions.offscreen,
                                         instructions.nbuffers);
    if (instructions.fieldHandle != NULL) {
        set_field_buffer(instructions.fieldHandle, 0, instructions.text);
        field_opts_off(instructions.fieldHandle, O_EDIT);
        field_opts_off(instructions.fieldHandle, O_ACTIVE);
    } else {
        fieldMissing = true;
    }

    label.fieldHandle = new_field(label.height, label.width, label.y,
                                  label.x, label.offscreen, label.nbuffers);
    if (label.fieldHandle != NULL) {
        set_field_buffer(label.fieldHandle, 0, label.text);
        field_opts_off(label.fieldHandle, O_EDIT);
        field_opts_off(label.fieldHandle, O_ACTIVE);
    } else {
        fieldMissing = true;
    }

    textInputField.x = label.width + 1;
    textInputField.fieldHandle = new_field(textInputField.height,
                                           textInputField.width,
                                           textInputField.y, textInputField.x,
                                           textInputField.offscreen,
                                           textInputField.nbuffers);
    if (textInputField.fieldHandle != NULL) {
        strlcpy(textInputBuf,
               selectedNode->dagNode->input->loadInput.loadArgs.sourceArgs.path,
                sizeof(textInputBuf));
        set_field_back(textInputField.fieldHandle, A_UNDERLINE);
        field_opts_off(textInputField.fieldHandle, O_AUTOSKIP);
        field_opts_off(textInputField.fieldHandle, O_BLANK);
        set_field_buffer(textInputField.fieldHandle, 0, textInputBuf);
    } else {
        fieldMissing = true;
    }

    FIELD *fields[4];
    fields[0] = instructions.fieldHandle;
    fields[1] = label.fieldHandle;
    fields[2] = textInputField.fieldHandle;
    fields[3] = NULL;

    if (fieldMissing) {
        goto CommonExit;
    }

    mainForm.formHandle = new_form(fields);
    if (mainForm.formHandle == NULL) {
        goto CommonExit;
    }
    form_opts_off(mainForm.formHandle, O_BS_OVERLOAD);

    scale_form(mainForm.formHandle, &mainForm.height, &mainForm.width);
    mainWindow.height = mainForm.height + mainForm.topPadding +
                        mainForm.botPadding;
    mainWindow.width = mainForm.width + mainForm.rightPadding +
                       mainForm.leftPadding;

    getmaxyx(stdscr, y, x);

    mainWindow.winHandle = newwin(mainWindow.height, mainWindow.width,
                                  (y - mainWindow.height) / 2,
                                  (x - mainWindow.width) / 2);
    if (mainWindow.winHandle == NULL) {
        goto CommonExit;
    }
    keypad(mainWindow.winHandle, TRUE);
    addChrome(mainWindow.winHandle, (char *)"Parameterize node");

    panel = new_panel(mainWindow.winHandle);
    if (panel == NULL) {
        goto CommonExit;
    }
    show_panel(panel);

    set_form_win(mainForm.formHandle, mainWindow.winHandle);
    set_form_sub(mainForm.formHandle, derwin(mainWindow.winHandle,
                                             mainForm.height, mainForm.width,
                                             mainForm.topPadding,
                                             mainForm.leftPadding));

    post_form(mainForm.formHandle);
    mainForm.formPosted = true;

    form_driver(mainForm.formHandle, REQ_FIRST_FIELD);
    form_driver(mainForm.formHandle, REQ_END_LINE);
    curs_set(1);

    wrefresh(mainWindow.winHandle);
    update_panels();
    doupdate();

    int ch;
    while (!exit) {
        ch = wgetch(mainWindow.winHandle);
        switch (ch) {
        case 27: // ESC or ALT
            nodelay(mainWindow.winHandle, true);
            ch = wgetch(mainWindow.winHandle);
            if (ch == ERR) {
                // That's how we know it's ESC
                exit = true;
            }
            nodelay(mainWindow.winHandle, false);
            break;
        case KEY_BACKSPACE:
            form_driver(mainForm.formHandle, REQ_DEL_PREV);
            break;
        case KEY_DC:
            form_driver(mainForm.formHandle, REQ_DEL_CHAR);
            break;
        case KEY_LEFT:
            form_driver(mainForm.formHandle, REQ_LEFT_CHAR);
            break;
        case KEY_RIGHT:
            form_driver(mainForm.formHandle, REQ_RIGHT_CHAR);
            break;
        case '\n':
        case KEY_ENTER:
            form_driver(mainForm.formHandle, REQ_NEXT_FIELD);
            assert(selectedNode->dagNode->hdr.api == XcalarApiBulkLoad);
            status = DagLib::setParamInput(paramInput,
                                        selectedNode->dagNode->hdr.api,
                          strTrim(field_buffer(textInputField.fieldHandle, 0)));
            if (status == StatusOk) {
                updateNode = true;
                exit = true;
            }
            break;
        default:
            form_driver(mainForm.formHandle, ch);
            break;
        }
    }

CommonExit:
    for (ii = 0; ii < ArrayLen(fields); ii++) {
        if (fields[ii] != NULL) {
            free_field(fields[ii]);
        }
        fields[ii] = NULL;
    }

    if (mainForm.formPosted) {
        assert(mainForm.formHandle != NULL);
        unpost_form(mainForm.formHandle);
        mainForm.formPosted = false;
    }

    if (mainForm.formHandle != NULL) {
        free_form(mainForm.formHandle);
        mainForm.formHandle = NULL;
    }

    if (panel != NULL) {
        del_panel(panel);
        panel = NULL;
    }

    if (mainWindow.winHandle != NULL) {
        delwin(mainWindow.winHandle);
        mainWindow.winHandle = NULL;
    }

    curs_set(0);
    refresh();
    return updateNode;
}

static Status
updateDagNode(const char *retinaName, DagNodeSprite *selectedNode,
              XcalarApiParamInput *paramInput)
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;

    workItem = xcalarApiMakeUpdateRetinaWorkItem(retinaName,
                                           selectedNode->dagNode->hdr.dagNodeId,
                                                 paramInput);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort, cliUsername,
                                cliUserIdUnique);

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static Status
getRetina(const char *retinaName, XcalarApiRetina **retinaOut)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    XcalarApiOutput *output = NULL;

    workItem = xcalarApiMakeGetRetinaWorkItem(retinaName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort, cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = workItem->output->hdr.status;
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Hand workItem->output over to output
    output = workItem->output;
    workItem->output = NULL;
    workItem->outputSize = 0;

    *retinaOut = &output->outputResult.getRetinaOutput.retina;
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static void
freeRetina(XcalarApiRetina *retina)
{
    XcalarApiGetRetinaOutput *getRetinaOutput;
    XcalarApiOutputResult *outputResult;
    XcalarApiOutput *output;

    assert(retina != NULL);

    getRetinaOutput = ContainerOf(retina, XcalarApiGetRetinaOutput,
                                  retina);

    outputResult = ContainerOf(getRetinaOutput, XcalarApiOutputResult,
                               getRetinaOutput);

    output = ContainerOf(outputResult, XcalarApiOutput,
                         outputResult);

    memFree(output);
    output = NULL;
}


static Status
displayRetina(const char *retinaName)
{
    Status status = StatusOk;
    XcalarApiRetina *retina = NULL;
    XcalarApiDagOutput *retinaDag;
    int ret;
    bool exitOuter = true;

    initscr();
    refresh();
    cbreak();
    keypad(stdscr, TRUE);
    noecho();
    curs_set(0);

    status = getRetina(retinaName, &retina);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(strcmp(retina->retinaDesc.retinaName, retinaName) == 0);

    // Start of drawing
    do {
        exitOuter = true;
        retinaDag = &retina->retinaDag;
        int retinaCanvasHeight = 100;
        int retinaCanvasWidth = 1000;
        char retinaCanvas[retinaCanvasHeight][retinaCanvasWidth];
        int ii, jj;

        for (jj = 0; jj < retinaCanvasHeight; jj++) {
            for (ii = 0; ii < retinaCanvasWidth; ii++) {
                retinaCanvas[jj][ii] = '\0';
            }
        }

        DagNodeSprite dagNodeSprites[retinaDag->numNodes];
        for (ii = 0; ii < (int) retinaDag->numNodes; ii++) {
            dagNodeSprites[ii].child = NULL;
            dagNodeSprites[ii].parent = NULL;
            dagNodeSprites[ii].nextSibling = &dagNodeSprites[ii];
            dagNodeSprites[ii].prevSibling = &dagNodeSprites[ii];
        }

        unserializeDagNodes(retinaDag, dagNodeSprites);
        positionDagNodeSprites(&dagNodeSprites[0]);
        status = drawDag(&retinaCanvas[0][0], retinaCanvasHeight,
                         retinaCanvasWidth, dagNodeSprites,
                         retinaDag->node, retinaDag->numNodes);
        if (status != StatusOk) {
            goto CommonExit;
        }

        int startX, startY;
        int windowWidth, windowHeight;
        DagNodeSprite *selectedNode;
        startX = startY = 0;
        windowHeight = 10;
        windowWidth = 70;

        bool exitInner = false;
        selectedNode = &dagNodeSprites[0];
        do {
            int ch;
            int x, y;
            bool parameterizable = false;

            clear();
            getmaxyx(stdscr, y, x);
            windowHeight = y / 2;
            windowWidth = x - 2;
            printw("Viewing retina \"%s\"\n\n", retinaName);
            printw("Use the arrow keys to scroll through the graph. Press ENTER"
                   " to parameterize the selected node.\n");
            printw("Use the keys W,A,S,D to pan through the graph. "
                   "Press P to view list of parameters.\n");
            printw("Press Q to quit.\n");
            displayCanvas(&retinaCanvas[0][0], retinaCanvasHeight,
                          retinaCanvasWidth, startX, startY, windowHeight,
                          windowWidth, selectedNode);
            printw("\n\n");
            printw("DagNodeTypes::Node: %s\n",
                   strGetFromXcalarApis(selectedNode->dagNode->hdr.api));
            printw("NodeId: %d\n", selectedNode->dagNode->hdr.dagNodeId);
            printw("Parent node: %s\n", (selectedNode->parent == NULL) ?
                   "None" :
                  strGetFromXcalarApis(selectedNode->parent->dagNode->hdr.api));
            printw("Child node: %s\n", (selectedNode->child == NULL) ?
                   "None" :
                   strGetFromXcalarApis(selectedNode->child->dagNode->hdr.api));

            parameterizable = isParameterizable(selectedNode->dagNode->hdr.api);
            printw("Parameterizable: %s\n", parameterizable ? "true" : "false");

            switch (selectedNode->dagNode->hdr.api) {
            case XcalarApiBulkLoad:
                printw("Dataset target: %s\n",
                       selectedNode->dagNode->input->loadInput.loadArgs
                       .sourceArgs.targetName);
                break;
            case XcalarApiIndex:
                printw("Index key: %s\n",
                       selectedNode->dagNode->input->
                       indexInput.keys[0].keyName);
                break;
            case XcalarApiFilter:
                printw("Eval statement: %s\n",
                       selectedNode->dagNode->input->filterInput.filterStr);
                break;
            default:
                break;
            }

            ch = getch();
            switch (ch) {
            case 'q':
                exitInner = true;
                break;
            case 'w':
                if (startY > 0) {
                    startY--;
                }
                break;
            case 's':
                if (startY + windowHeight < retinaCanvasHeight) {
                    startY++;
                }
                break;
            case 'a':
                if (startX > 0) {
                    startX--;
                }
                break;
            case 'd':
                if (startX + windowWidth < retinaCanvasWidth) {
                    startX++;
                }
                break;
            case KEY_LEFT:
                if (selectedNode->child != NULL) {
                    selectedNode = selectedNode->child;
                }
                break;
            case KEY_RIGHT:
                if (selectedNode->parent != NULL) {
                    selectedNode = selectedNode->parent;
                }
                break;
            case KEY_DOWN:
                selectedNode = selectedNode->nextSibling;
                break;
            case KEY_UP:
                selectedNode = selectedNode->prevSibling;
                break;
            case '\n':
            case KEY_ENTER:
                if (parameterizable) {
                    XcalarApiParamInput paramInput;
                    bool updateNode;
                    updateNode = showParamNodeWindow(selectedNode, &paramInput);
                    if (updateNode) {
                        status = updateDagNode(retinaName, selectedNode,
                                               &paramInput);
                        if (status != StatusOk) {
                            goto CommonExit;
                        }

                        assert(retina != NULL);
                        freeRetina(retina);
                        retina = NULL;

                        status = getRetina(retinaName, &retina);
                        if (status != StatusOk) {
                            goto CommonExit;
                        }

                        exitOuter = false;
                        exitInner = true;
                    }
                }
                break;
            }
        } while (!exitInner);
    } while (!exitOuter);

    printf("Showing retina %s. Num nodes: %lu\n", retinaName,
            retinaDag->numNodes);

CommonExit:
    ret = endwin();
    assert(ret == OK);

    if (retina != NULL) {
        freeRetina(retina);
        retina = NULL;
    }

    return status;
}

static Status
pickColumnNames(char tableNameArray[][DagTypes::MaxNameLen + 1],
                uint64_t numTables, RetinaDst ***dstArrayOut)
{
    return StatusUnimpl;
}

static void
retinaMakeHelp(const char *moduleName, int argc, char *argv[])
{
    printf("Usage: %s %s <retinaName>\n",
           moduleName, argv[0]);
}

static void
retinaMake(const char *moduleName, int argc, char *argv[], bool prettyPrint,
           bool interactive)
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;
    char tableNameArray[XcalarApiRetinaMaxNumTables][DagTypes::MaxNameLen + 1];
    uint64_t ii, numTables = 0;
    int jj, scanfRet;
    const char *retinaName = NULL;
    RetinaDst **dstArray = NULL;

    if (argc < 2) {
        fprintf(stderr, "Retina name required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    retinaName = argv[1];
    if (strlen(retinaName) == 0) {
        fprintf(stderr, "Please enter a valid retina name\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (interactive) {
        status = pickTableNames(tableNameArray, &numTables);
        if (status != StatusOk || numTables == 0) {
            goto CommonExit;
        }

        assert(numTables > 0);
        status = pickColumnNames(tableNameArray, numTables, &dstArray);
        if (status != StatusOk) {
            goto CommonExit;
        }
    } else {
        printf("Enter number of tables: ");
        scanfRet = scanf("%lu", &numTables);
        if (scanfRet < 0) {
            fprintf(stderr, "Number of tables is invalid\n");
            status = StatusInval;
            goto CommonExit;
        }

        if (numTables > XcalarApiRetinaMaxNumTables) {
            fprintf(stderr, "Too many tables. Max is %u tables\n",
                    XcalarApiRetinaMaxNumTables);
            status = StatusNoBufs;
            goto CommonExit;
        }

        dstArray = (RetinaDst **) memAlloc(sizeof(dstArray[0]) * numTables);
        if (dstArray == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        for (ii = 0; ii < numTables; ii++) {
            dstArray[ii] = NULL;
        }

        for (ii = 0; ii < numTables; ii++) {
            char format[32];
            int numColumns;
            size_t ret;

            printf("Enter name of table %lu: ", ii + 1);
            ret = snprintf(format, sizeof(format), "%%%lus",
                           sizeof(tableNameArray[0]) - 1);
            assert(ret < sizeof(format));

            scanfRet = scanf(format, tableNameArray[ii]);
            if (scanfRet < 0) {
                fprintf(stderr, "Name of table is invalid\n");
                status = StatusInval;
                goto CommonExit;
            }

            printf("Enter number of columns for table %s: ",
                   tableNameArray[ii]);
            scanfRet = scanf("%d", &numColumns);
            if (scanfRet < 0) {
                fprintf(stderr, "Number of columns is invalid\n");
                status = StatusInval;
                goto CommonExit;
            }

            dstArray[ii] = (RetinaDst *) memAlloc(sizeof(*dstArray[ii]) +
                                             (sizeof(dstArray[ii]->columns[0]) *
                                                  numColumns));
            if (dstArray[ii] == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }

            dstArray[ii]->numColumns = numColumns;
            dstArray[ii]->target.isTable = true;
            ret = strlcpy(dstArray[ii]->target.name, tableNameArray[ii],
                          sizeof(dstArray[ii]->target.name));
            if (ret >= sizeof(dstArray[ii]->target.name)) {
                fprintf(stderr, "Table name is too long. Max is %lu characters",
                        sizeof(dstArray[ii]->target.name) - 1);
                status = StatusNoBufs;
                goto CommonExit;
            }

            for (jj = 0; jj < numColumns; jj++) {
                printf("Table %s column %d -- enter name: ", tableNameArray[ii],
                       jj + 1);
                ret = snprintf(format, sizeof(format), "%%%lus",
                               sizeof(dstArray[ii]->columns[jj].name) - 1);
                assert(ret < sizeof(format));

                scanfRet = scanf(format, dstArray[ii]->columns[jj].name);
                if (scanfRet < 0) {
                    fprintf(stderr, "Name of column is invalid\n");
                    status = StatusInval;
                    goto CommonExit;
                }

                printf("Table %s column %d -- enter alias: ",
                       tableNameArray[ii], jj + 1);
                ret = snprintf(format, sizeof(format), "%%%lus",
                               sizeof(dstArray[ii]->columns[jj].headerAlias)
                               - 1);
                assert(ret < sizeof(format));

                scanfRet = scanf(format, dstArray[ii]->columns[jj].headerAlias);
                if (scanfRet < 0) {
                    fprintf(stderr, "Column alias name is invalid\n");
                    status = StatusInval;
                    goto CommonExit;
                }

            }

        }
    }

    workItem = xcalarApiMakeMakeRetinaWorkItem(retinaName, numTables,
                                               dstArray, 0, NULL);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    workItem->legacyClient = true;

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort,
                                cliUsername, cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = workItem->output->hdr.status;
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (interactive) {
        // Now we need to customize the retina;
        status = displayRetina(retinaName);
    } else {
        printf("Retina %s created successfully\n", retinaName);
        status = StatusOk;
    }

CommonExit:
    if (dstArray != NULL) {
        for (ii = 0; ii < numTables; ii++) {
            if (dstArray[ii] != NULL) {
                memFree(dstArray[ii]);
                dstArray[ii] = NULL;
            }
        }
        memFree(dstArray);
        dstArray = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status == StatusCliParseError) {
        retinaMakeHelp(moduleName, argc, argv);
    } else if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

static void
retinaListHelp(const char *moduleName, int argc, char *argv[])
{
    printf("Usage: %s %s\n", moduleName, argv[0]);
}

static void
retinaList(const char *moduleName, int argc, char *argv[], bool prettyPrint,
           bool interactive)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiListRetinasOutput *listRetinasOutput = NULL;
    Status status = StatusUnknown;
    unsigned ii;

    workItem = xcalarApiMakeListRetinasWorkItem("*");
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort, cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    listRetinasOutput = &workItem->output->outputResult.listRetinasOutput;
    if (workItem->output->hdr.status != StatusOk) {
        fprintf(stderr, "Error. Server returned: %s\n",
                strGetFromStatus(workItem->output->hdr.status));
    } else if (prettyPrint) {
        assert(workItem->output->hdr.status == StatusOk);
        printf("Retina Name\n");
        printf("===========================================\n");
        if (listRetinasOutput->numRetinas == 0) {
            printf("No retinas found\n");
        } else {
            for (ii = 0; ii < listRetinasOutput->numRetinas; ii++) {
                printf("%s\n", listRetinasOutput->retinaDescs[ii].retinaName);
            }
        }
    } else {
        assert(workItem->output->hdr.status == StatusOk);
        assert(!prettyPrint);
        printf("Number of retinas: %llu\n",
               (unsigned long long) listRetinasOutput->numRetinas);
        printf("Retina name\n");
        for (ii = 0; ii < listRetinasOutput->numRetinas; ii++) {
            printf("\"%s\"\n", listRetinasOutput->retinaDescs[ii].retinaName);
        }
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        listRetinasOutput = NULL;
    }

    if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

static void
retinaDeleteHelp(const char *moduleName, int argc, char *argv[])
{
    printf("Usage: %s %s <retinaName>\n", moduleName, argv[0]);
}

static void
retinaDelete(const char *moduleName, int argc, char *argv[], bool prettyPrint,
              bool interactive)
{
    Status status = StatusUnknown;
    const char *retinaName;
    XcalarWorkItem *workItem = NULL;

    if (argc != 2) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    retinaName = argv[1];
    if (strlen(retinaName) == 0) {
        fprintf(stderr, "Please enter a valid <retinaName>\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    workItem = xcalarApiMakeDeleteRetinaWorkItem(retinaName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort,
                                cliUsername, cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (workItem->output->hdr.status != StatusOk) {
        fprintf(stderr, "Error. Server returned: %s\n",
                strGetFromStatus(workItem->output->hdr.status));
        goto CommonExit;
    }

    printf("Retina \"%s\" deleted successfully\n", retinaName);

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status == StatusCliParseError) {
        retinaDeleteHelp(moduleName, argc, argv);
    } else if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

static Status
getDagNodeParamType(const char *retinaName, XcalarApiDagNodeId dagNodeId,
                    XcalarApis *paramTypeOut)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    XcalarApiGetRetinaOutput *getRetinaOutput = NULL;
    XcalarApis paramType;
    unsigned ii;

    assert(retinaName != NULL);
    assert(paramTypeOut != NULL);

    workItem = xcalarApiMakeGetRetinaWorkItem(retinaName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort, cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    getRetinaOutput = &workItem->output->outputResult.getRetinaOutput;
    status = workItem->output->hdr.status;
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = StatusDagNodeNotFound;
    for (ii = 0; ii < getRetinaOutput->retina.retinaDag.numNodes; ii++) {
        if (getRetinaOutput->retina.retinaDag.node[ii]->hdr.dagNodeId
                                                                 == dagNodeId) {
            paramType = getRetinaOutput->retina.retinaDag.node[ii]->hdr.api;
            status = StatusOk;
            break;
        }
    }

    if (status != StatusOk) {
        goto CommonExit;
    }

    *paramTypeOut = paramType;

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        getRetinaOutput = NULL;
    }

    return status;
}

static void
retinaUpdateHelp(const char *moduleName, int argc, char *argv[])
{
    printf("Usage: %s %s <retinaName> <retinaNodeId> <newValue>\n",
           moduleName, argv[0]);
}

static void
retinaUpdate(const char *moduleName, int argc, char *argv[], bool prettyPrint,
             bool interactive)
{
    Status status = StatusUnknown;
    Status *statusOutput;
    XcalarWorkItem *workItem = NULL;
    const char *retinaName = NULL;
    char *endPtr = NULL;
    XcalarApiDagNodeId dagNodeId;
    XcalarApis paramType = XcalarApiUnknown;
    XcalarApiParamInput paramInput;

    if (argc != 4) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    retinaName = argv[1];
    if (strlen(retinaName) == 0) {
        status = StatusCliParseError;
        fprintf(stderr, "Please enter a valid <retinaName>\n");
        goto CommonExit;
    }

    dagNodeId = strtoull(argv[2], &endPtr, BaseCanonicalForm);
    if (endPtr == argv[2]) {
        status = StatusCliParseError;
        fprintf(stderr, "Please enter a valid <dagNodeId>\n");
        goto CommonExit;
    }

    status = getDagNodeParamType(retinaName, dagNodeId, &paramType);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(paramType != XcalarApiUnknown);
    status = DagLib::setParamInput(&paramInput, paramType, argv[3]);

    workItem = xcalarApiMakeUpdateRetinaWorkItem(retinaName, dagNodeId,
                                                 &paramInput);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort, cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    statusOutput = &workItem->output->hdr.status;
    if (*statusOutput == StatusOk) {
        printf("Retina \"%s\" updated successfully\n", retinaName);
    } else {
        fprintf(stderr, "Error. Server returned: %s\n",
                strGetFromStatus(*statusOutput));
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status == StatusCliParseError) {
        retinaUpdateHelp(moduleName, argc, argv);
    } else if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

static void
retinaListParamsHelp(const char *moduleName, int argc, char *argv[])
{
    printf("Usage: %s %s <retinaName>\n", moduleName, argv[0]);
}

static void
retinaListParams(const char *moduleName, int argc, char *argv[],
                 bool prettyPrint, bool interactive)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    XcalarApiListParametersInRetinaOutput *listParametersInRetinaOutput = NULL;
    const char *retinaName = NULL;
    unsigned ii;

    if (argc != 2) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    retinaName = argv[1];
    if (strlen(retinaName) == 0) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    workItem = xcalarApiMakeListParametersInRetinaWorkItem(retinaName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort, cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    listParametersInRetinaOutput =
                   &workItem->output->outputResult.listParametersInRetinaOutput;
    if (workItem->output->hdr.status != StatusOk) {
        fprintf(stderr, "Error. Server returned: %s\n",
                strGetFromStatus(workItem->output->hdr.status));
        goto CommonExit;
    }

    if (prettyPrint) {
        printf("%-30s%-30s\n", "Parameter name", "Parameter default value");
        printf("=======================================================\n");
        if (listParametersInRetinaOutput->numParameters == 0) {
            printf("No default parameters found\n");
        } else {
            for (ii = 0; ii < listParametersInRetinaOutput->numParameters;
                 ii++) {
                printf("%-30s%-30s\n",
                     listParametersInRetinaOutput->parameters[ii].parameterName,
                   listParametersInRetinaOutput->parameters[ii].parameterValue);
            }
        }
    } else {
        printf("Number of parameters: %lu\n",
               listParametersInRetinaOutput->numParameters);
        for (ii = 0; ii < listParametersInRetinaOutput->numParameters; ii++) {
            printf("\"%s\"\t\"%s\"\n",
                   listParametersInRetinaOutput->parameters[ii].parameterName,
                   listParametersInRetinaOutput->parameters[ii].parameterValue);
        }
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        listParametersInRetinaOutput = NULL;
    }

    if (status == StatusCliParseError) {
        retinaListParamsHelp(moduleName, argc, argv);
    } else if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

static void
retinaLoadHelp(const char *moduleName, int argc, char *argv[])
{
    printf("Usage: %s %s --retinaName <retinaName> "
           "--retinaPath <path-to-retina-file/folder> [--verbose] "
           "[--force]\n", moduleName, argv[0]);
}

struct RetinaLoadSaveArgs {
    const char *retinaName;
    const char *retinaPath;
    bool verbose;
    bool forceful;
};

static Status
parseRetinaLoadSaveArgs(int argc, char *argv[],
                        RetinaLoadSaveArgs *retinaLoadSaveArgs)
{
    int optionIndex = 0;
    int flag;
    OptionThr longOptions[] = {
        { "retinaName", required_argument, 0, 'n' },
        { "retinaPath", required_argument, 0, 'r' },
        { "verbose", no_argument, 0, 'v' },
        { "force", no_argument, 0, 'f' },
        { 0, 0, 0, 0 },
    };
    GetOptDataThr optData;

    retinaLoadSaveArgs->retinaName = NULL;
    retinaLoadSaveArgs->retinaPath = NULL;
    retinaLoadSaveArgs->verbose = false;
    retinaLoadSaveArgs->forceful = false;

    getOptLongInit();
    while ((flag = getOptLong(argc, argv, "n:r:vf", longOptions,
                              &optionIndex, &optData)) != -1) {
        switch (flag) {
        case 'n':
            if (retinaLoadSaveArgs->retinaName == NULL) {
                retinaLoadSaveArgs->retinaName = optData.optarg;
            } else {
                fprintf(stderr, "<retinaName> provided multiple times. "
                        "Using the first one\n");
            }
            break;
        case 'r':
            if (retinaLoadSaveArgs->retinaPath == NULL) {
                retinaLoadSaveArgs->retinaPath = optData.optarg;
            } else {
                fprintf(stderr, "<retinaPath> provided multiple times. "
                        "Using the first one\n");
            }
            break;
        case 'v':
            retinaLoadSaveArgs->verbose = true;
            break;
        case 'f':
            retinaLoadSaveArgs->forceful = true;
            break;
        default:
            return StatusCliParseError;
        }
    }

    if (retinaLoadSaveArgs->retinaName == NULL) {
        fprintf(stderr, "Error: retinaName not specified!\n");
        return StatusCliParseError;
    }

    if (retinaLoadSaveArgs->retinaPath == NULL) {
        fprintf(stderr, "Error: retinaPath not specified!\n");
        return StatusCliParseError;
    }

    return StatusOk;
}

static Status
addDirectoryToManifest(ArchiveManifest *manifest, const char *dirPrefix,
                       const char *retinaPath, bool verbose)
{
    DIR *dir = NULL;
    struct dirent *dp = NULL;
    char newDirPrefix[XcalarApiMaxPathLen + 1];
    char currDirectory[XcalarApiMaxPathLen + 1];
    char filePath[XcalarApiMaxPathLen + 1];
    char relativeFilePath[XcalarApiMaxPathLen + 1];
    size_t ret;
    int retVal;
    struct stat statBuf;
    FILE *fp = NULL;
    size_t fileSize;
    void *fileContents = NULL;
    Status status = StatusUnknown;

    assert(manifest != NULL);
    assert(dirPrefix != NULL);
    assert(retinaPath != NULL);

    ret = snprintf(currDirectory, sizeof(currDirectory), "%s%s",
                   retinaPath, dirPrefix);
    if (ret >= sizeof(currDirectory)) {
        status = StatusNameTooLong;
        goto CommonExit;
    }

    errno = 0;
    dir = opendir(currDirectory);
    if (dir == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    while ((dp = readdir(dir)) != NULL) {
        if (strcmp(dp->d_name, ".") == 0 ||
            strcmp(dp->d_name, "..") == 0) {
            continue;
        }

        ret = snprintf(filePath, sizeof(filePath), "%s%s",
                       currDirectory, dp->d_name);
        if (ret >= sizeof(filePath)) {
            status = StatusNameTooLong;
            goto CommonExit;
        }

        if (verbose) {
            printf("Adding %s\n", filePath);
        }

        retVal = stat(filePath, &statBuf);
        if (retVal != 0) {
            status = sysErrnoToStatus(errno);
            goto CommonExit;
        }

        if (S_ISDIR(statBuf.st_mode)) {
            ret = snprintf(newDirPrefix, sizeof(newDirPrefix), "%s%s/",
                           dirPrefix, dp->d_name);
            if (ret >= sizeof(newDirPrefix)) {
                status = StatusNameTooLong;
                goto CommonExit;
            }

            status = addDirectoryToManifest(manifest, newDirPrefix, retinaPath,
                                            verbose);
            if (status != StatusOk) {
                goto CommonExit;
            }
        } else {
            ret = snprintf(relativeFilePath, sizeof(relativeFilePath), "%s%s",
                           dirPrefix, dp->d_name);
            if (ret >= sizeof(relativeFilePath)) {
                status = StatusNameTooLong;
                goto CommonExit;
            }

            // @SymbolCheckIgnore
            fp = fopen(filePath, "r");
            if (fp == NULL) {
                fprintf(stderr, "Could not open %s\n", filePath);
                status = sysErrnoToStatus(errno);
                goto CommonExit;
            }

            fileSize = statBuf.st_size;

            if (fileSize > XcalarApiMaxUdfSourceLen) {
                fprintf(stderr, "%s is too big (%lu bytes). Max is %u bytes\n",
                        filePath, fileSize, XcalarApiMaxUdfSourceLen);
                status = StatusNoBufs;
                goto CommonExit;
            }

            fileContents = (void *) memAlloc(fileSize);
            if (fileContents == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }

            // @SymbolCheckIgnore
            if (fread(fileContents, 1, fileSize, fp) != fileSize) {
                fprintf(stderr, "Error reading %s\n", filePath);
                status = sysErrnoToStatus(errno);
                goto CommonExit;
            }

            status = archiveAddFileToManifest(relativeFilePath, fileContents,
                                              fileSize, manifest, NULL);
            if (status != StatusOk) {
                goto CommonExit;
            }

            assert(fp != NULL);

            // @SymbolCheckIgnore
            fclose(fp);
            fp = NULL;

            assert(fileContents != NULL);
            memFree(fileContents);
            fileContents = NULL;
        }
    }

CommonExit:
    if (fileContents != NULL) {
        assert(status != StatusOk);
        memFree(fileContents);
        fileContents = NULL;
    }

    if (fp != NULL) {
        assert(status != StatusOk);
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;
    }

    if (dir != NULL) {
        closedir(dir);
        dir = NULL;
    }

    return status;
}

static Status
loadRetinaFromDirectory(const char *retinaPathIn, uint8_t **retinaBufOut,
                        size_t *retinaBufSizeOut, bool verbose)
{
    Status status = StatusUnknown;
    ArchiveManifest *archiveManifest = NULL;
    const char *retinaPath;
    char tempRetinaPath[XcalarApiMaxPathLen + 1];
    size_t retinaPathLen, ret;

    assert(retinaPathIn != NULL);
    assert(retinaBufOut !=  NULL);
    assert(retinaBufSizeOut != NULL);

    retinaPathLen = strlen(retinaPathIn);
    if (retinaPathIn[retinaPathLen - 1] != '/') {
        ret = snprintf(tempRetinaPath, sizeof(tempRetinaPath), "%s/",
                       retinaPathIn);
        if (ret >= sizeof(tempRetinaPath)) {
            status = StatusNameTooLong;
            goto CommonExit;
        }
        retinaPath = tempRetinaPath;
    } else {
        retinaPath = retinaPathIn;
    }

    archiveManifest = archiveNewEmptyManifest();
    if (archiveManifest == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = addDirectoryToManifest(archiveManifest, "", retinaPath,
                                    verbose);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = archivePack(archiveManifest, (void **) retinaBufOut,
                         retinaBufSizeOut);
CommonExit:
    if (archiveManifest != NULL) {
        archiveFreeManifest(archiveManifest);
        archiveManifest = NULL;
    }

    return status;
}

static void
retinaLoad(const char *moduleName, int argc, char *argv[], bool prettyPrint,
           bool interactive)
{
    RetinaLoadSaveArgs retinaLoadSaveArgs;
    Status status = StatusUnknown;
    struct stat statBuf;
    int ret;
    uint8_t *retinaBuf = NULL;
    size_t retinaBufSize = 0;
    RetinaInfo *retinaInfo = NULL;
    XcalarWorkItem *workItem = NULL;

    status = parseRetinaLoadSaveArgs(argc, argv, &retinaLoadSaveArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }

    ret = stat(retinaLoadSaveArgs.retinaPath, &statBuf);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    if (S_ISDIR(statBuf.st_mode)) {
        status = loadRetinaFromDirectory(retinaLoadSaveArgs.retinaPath,
                                            &retinaBuf, &retinaBufSize,
                                            retinaLoadSaveArgs.verbose);
    } else {
        status = DagLib::loadRetinaFromFile(retinaLoadSaveArgs.retinaPath,
                                            &retinaBuf, &retinaBufSize,
                                            &statBuf);
    }

    if (status != StatusOk) {
        goto CommonExit;
    }

    // Do some sanity check on the retina
    assert(retinaBuf != NULL);
    assert(retinaBufSize > 0);
    status = DagLib::parseRetinaFile(retinaBuf, retinaBufSize,
                                        retinaLoadSaveArgs.retinaName,
                                        &retinaInfo);
    if (status != StatusOk) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeImportRetinaWorkItem(retinaLoadSaveArgs.retinaName,
                                                 retinaLoadSaveArgs.forceful,
                                                 false, NULL, NULL,
                                                 retinaBufSize, retinaBuf)
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort, cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(status == StatusOk);
    status = workItem->output->hdr.status;
    if (status != StatusOk) {
        goto CommonExit;
    }

    printf("Retina \"%s\" uploaded successfully\n",
           retinaLoadSaveArgs.retinaName);
CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (retinaInfo != NULL) {
        DagLib::destroyRetinaInfo(retinaInfo);
        retinaInfo = NULL;
    }

    if (retinaBuf != NULL) {
        memFree(retinaBuf);
        retinaBuf = NULL;
    }

    if (status == StatusCliParseError) {
        retinaLoadHelp(moduleName, argc, argv);
    } else if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

static void
retinaSave(const char *moduleName, int argc, char *argv[], bool prettyPrint,
           bool interactive)
{
    RetinaLoadSaveArgs retinaLoadSaveArgs;
    Status status = StatusUnknown;
    size_t bytesWritten = 0;
    XcalarApiExportRetinaOutput *exportRetinaOutput = NULL;
    XcalarWorkItem *workItem = NULL;
    FILE *fp = NULL;

    status = parseRetinaLoadSaveArgs(argc, argv, &retinaLoadSaveArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (!retinaLoadSaveArgs.forceful) {
        // @SymbolCheckIgnore
        fp = fopen(retinaLoadSaveArgs.retinaPath, "r");
        if (fp != NULL) {
            status = StatusExist;
            fprintf(stderr, "%s already exists. Use --force to override\n",
                    retinaLoadSaveArgs.retinaPath);
            goto CommonExit;
        }
    }

    errno = 0;
    // @SymbolCheckIgnore
    fp = fopen(retinaLoadSaveArgs.retinaPath, "wb");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    workItem = xcalarApiMakeExportRetinaWorkItem(retinaLoadSaveArgs.retinaName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem, cliDestIp, cliDestPort, cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = workItem->output->hdr.status;
    if (status != StatusOk) {
        goto CommonExit;
    }

    exportRetinaOutput = &workItem->output->outputResult.exportRetinaOutput;
    // @SymbolCheckIgnore
    bytesWritten = fwrite(exportRetinaOutput->retina, 1,
                          exportRetinaOutput->retinaCount, fp);
    if (ferror(fp)) {
        status = StatusIO;
        fprintf(stderr, "Could not write to %s\n",
                retinaLoadSaveArgs.retinaPath);
        goto CommonExit;
    }

    printf("Retina %s saved successfully to %s. %lu bytes written\n",
           retinaLoadSaveArgs.retinaName, retinaLoadSaveArgs.retinaPath,
           bytesWritten);
    status = StatusOk;
CommonExit:
    if (fp != NULL) {
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        exportRetinaOutput = NULL;
    }

    if (status == StatusCliParseError) {
        retinaSaveHelp(moduleName, argc, argv);
    } else if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

static void
retinaSaveHelp(const char *moduleName, int argc, char *argv[])
{
    printf("Usage: %s %s --retinaName <retinaName> "
           "--retinaPath <path-to-retina-file> [--verbose] "
           "[--force]\n", moduleName, argv[0]);
}
