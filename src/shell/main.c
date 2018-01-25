/*****************************************************************************
 *
 *																 chidb
 *
 * This module provides a simple chidb shell.
 *
 * This shell assumes a complete implementation of the chidb API
 * is available. If so, provides a basic SQL shell.
 *
 * 2009, 2010 Borja Sotomayor - http://people.cs.uchicago.edu/~borja/
\*****************************************************************************/
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <chidb/chidb.h>
#include <chidb/log.h>
#include "shell.h"
#include "commands.h"

#define  MAX_CMD 512

int main(int argc, char *argv[]) {
    int opt;
    int rc;
    int verbosity = 0;
    char *command = "select sno from student;";
    chidb_shell_ctx_t shell_ctx;

    chidb_shell_init_ctx(&shell_ctx);

    /* Process command-line arguments */
    while ((opt = getopt(argc, argv, "c:vh")) != -1)
        switch (opt) {
            case 'c':
                command = strdup(optarg);
                break;
            case 'v':
                verbosity++;
                break;
            case 'h':
                printf("Usage: chidb [-c COMMAND] [DATABASE]\n");
                exit(0);
            default:
                printf("ERROR: Unknown option -%c\n", opt);
                exit(-1);
        }

    /* Set logging level based on verbosity */
    switch (verbosity) {
        case 0:
            chilog_setloglevel(CRITICAL);
            break;
        case 1:
            chilog_setloglevel(INFO);
            break;
        case 2:
            chilog_setloglevel(DEBUG);
            break;
        case 3:
            chilog_setloglevel(TRACE);
            break;
        default:
            chilog_setloglevel(TRACE);
            break;
    }

    char cmdstring[MAX_CMD];
    int n;
    while (1) {
        printf("*chidb*>");

        if ((n = read(0, cmdstring, MAX_CMD)) < 0) {
            printf("read error");
        }

        int wordcount = 0;
        while(cmdstring[wordcount] != '\n'){
            wordcount++;
        }
        cmdstring[wordcount] = '\0';

        if (cmdstring) {
            chidb_shell_handle_cmd(&shell_ctx, cmdstring);
        }

    }

//    optind= 0;
//    char *file = "demo.db";
//    if (optind < argc)
//    {
//        rc = chidb_shell_open_db(&shell_ctx, file /*argv[optind]*/);
//        if(rc)
//        {
//            fprintf(stderr, "ERROR: Could not open file %s or file is not well formed.\n", argv[optind]);
//            exit(1);
//        }
//    }

    /* If a command was specified as an argument, we just run that.
     * Otherwise, we start the shell. */



    return 0;
}

