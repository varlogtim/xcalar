/*
 * Derived from:
 *
 * https://github.com/freebsd/freebsd/blob/master/lib/libc/stdlib/getopt_long.c
 */

/*	$OpenBSD: getopt_long.c,v 1.26 2013/06/08 22:47:56 millert Exp $	*/
/*	$NetBSD: getopt_long.c,v 1.15 2002/01/31 22:43:40 tv Exp $	*/

/*
 * Copyright (c) 2002 Todd C. Miller <Todd.Miller@courtesan.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Sponsored in part by the Defense Advanced Research Projects
 * Agency (DARPA) and Air Force Research Laboratory, Air Force
 * Materiel Command, USAF, under agreement number F39502-99-1-0512.
 */
/*-
 * Copyright (c) 2000 The NetBSD Foundation, Inc.
 * All rights reserved.
 *
 * This code is derived from software contributed to The NetBSD Foundation
 * by Dieter Baron and Thomas Klausner.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE NETBSD FOUNDATION, INC. AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE FOUNDATION OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * Note on extensions by Xcalar:
 *
 * This code is derived from the BSD implementation as noted above. It has been
 * made thread-safe and re-entrant using TLS, and a new interface added for
 * this purpose: getOptLongInit().
 *
 * Look for the string "XC" which flags enhancements/changes by Xcalar.
 */

#if 0
#if defined(LIBC_SCCS) && !defined(lint)
static char *rcsid = "$OpenBSD: getopt_long.c,v 1.16 2004/02/04 18:17:25 millert Exp $";
#endif /* LIBC_SCCS and not lint */
#endif

#include <sys/cdefs.h>
#include <err.h>
#include <errno.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include "GetOpt.h" /* XC: replaces the system getopt.h */

#define GNU_COMPATIBLE		/* Be more compatible, configure's use us! */

#if 1				/* we prefer to keep our getopt(3) */
#define	REPLACE_GETOPT		/* use this getopt as the system getopt(3) */
#endif

#define	EMSG		""

/*
 * XC: make the static globals thread-local using the "__thread" class, and
 * collect all the __thread class variables in once place as far as possible.
 * Add the _tls suffix to distinguish them from names in the system's getopt.h.
 * Use of TLS allows the code to remain almost identical to the original which
 * used static globals (e.g. references to "globals" such as opterr or optind
 * remain identical in the thread-safe version).
 */
#ifdef REPLACE_GETOPT
__thread int opterr_tls = 1;		/* if error message should be printed */
__thread int optind_tls = 1;		/* index into parent argv vector */
__thread int optopt_tls = '?';		/* character checked for validity */
__thread int optreset_tls;		    /* reset getopt */
__thread char *optarg_tls;		    /* argument associated with option */
#endif
__thread char *place = EMSG;        /* option letter processing */
__thread int posixly_correct = -1;  /* XC: moved outside func: it was static */

/* XXX: set optreset_tls to 1 rather than these two */

__thread int nonopt_start = -1; /* first non option argument (for permute) */
__thread int nonopt_end = -1; /* first option after non options (for permute) */

#define PRINT_ERROR	(opterr_tls && (*options != ':'))

#define FLAG_PERMUTE	0x01	/* permute non-options to the end of argv */
#define FLAG_ALLARGS	0x02	/* treat non-options as args to option "-1" */
#define FLAG_LONGONLY	0x04	/* operate as getopt_long_only */

/* return values */
#define	BADCH		(int)'?'
#define	BADARG		((*options == ':') ? (int)':' : (int)'?')
#define	INORDER 	(int)1

#ifdef GNU_COMPATIBLE
#define NO_PREFIX	(-1)
#define D_PREFIX	0
#define DD_PREFIX	1
#define W_PREFIX	2
#endif

static int getopt_internal(int, char * const *, const char *,
			   const OptionThr *, int *, int, GetOptDataThr *);
static int parse_long_options(char * const *, const char *,
			      const OptionThr *, int *, int, int, GetOptDataThr *);
static int gcd(int, int);
static void permute_args(int, int, int, char * const *);



/* Error messages */
static const char recargchar[] = "option requires an argument -- %c";
static const char illoptchar[] = "illegal option -- %c"; /* From P1003.2 */
#ifdef GNU_COMPATIBLE
__thread int dash_prefix = NO_PREFIX;
static const char gnuoptchar[] = "invalid option -- %c";

static const char recargstring[] = "option `%s%s' requires an argument";
static const char ambig[] = "option `%s%.*s' is ambiguous";
static const char noarg[] = "option `%s%.*s' doesn't allow an argument";
static const char illoptstring[] = "unrecognized option `%s%s'";
#else
static const char recargstring[] = "option requires an argument -- %s";
static const char ambig[] = "ambiguous option -- %.*s";
static const char noarg[] = "option doesn't take an argument -- %.*s";
static const char illoptstring[] = "unknown option -- %s";
#endif

/*
 * Compute the greatest common divisor of a and b.
 */
static int
gcd(int a, int b)
{
	int c;

	c = a % b;
	while (c != 0) {
		a = b;
		b = c;
		c = a % b;
	}

	return (b);
}

/*
 * Exchange the block from nonopt_start to nonopt_end with the block
 * from nonopt_end to opt_end (keeping the same order of arguments
 * in each block).
 */
static void
permute_args(int panonopt_start, int panonopt_end, int opt_end,
	char * const *nargv)
{
	int cstart, cyclelen, i, j, ncycle, nnonopts, nopts, pos;
	char *swap;

	/*
	 * compute lengths of blocks and number and size of cycles
	 */
	nnonopts = panonopt_end - panonopt_start;
	nopts = opt_end - panonopt_end;
	ncycle = gcd(nnonopts, nopts);
	cyclelen = (opt_end - panonopt_start) / ncycle;

	for (i = 0; i < ncycle; i++) {
		cstart = panonopt_end+i;
		pos = cstart;
		for (j = 0; j < cyclelen; j++) {
			if (pos >= panonopt_end)
				pos -= nnonopts;
			else
				pos += nopts;
			swap = nargv[pos];
			/* LINTED const cast */
			((char **) nargv)[pos] = nargv[cstart];
			/* LINTED const cast */
			((char **)nargv)[cstart] = swap;
		}
	}
}

/*
 * parse_long_options --
 *	Parse long options in argc/argv argument vector.
 * Returns -1 if short_too is set and the option does not match long_options.
 */
static int
parse_long_options(char * const *nargv, const char *options,
	const OptionThr *long_options, int *idx, int short_too, int flags,
    GetOptDataThr *goptsd)
{
	char *current_argv, *has_equal;
#ifdef GNU_COMPATIBLE
	char *current_dash;
#endif
	size_t current_argv_len;
	int i, match, exact_match, second_partial_match;

	current_argv = place;
#ifdef GNU_COMPATIBLE
	switch (dash_prefix) {
		case D_PREFIX:
			current_dash = "-";
			break;
		case DD_PREFIX:
			current_dash = "--";
			break;
		case W_PREFIX:
			current_dash = "-W ";
			break;
		default:
			current_dash = "";
			break;
	}
#endif
	match = -1;
	exact_match = 0;
	second_partial_match = 0;

	optind_tls++;

	if ((has_equal = strchr(current_argv, '=')) != NULL) {
		/* argument found (--option=arg) */
		current_argv_len = has_equal - current_argv;
		has_equal++;
	} else
		current_argv_len = strlen(current_argv);

	for (i = 0; long_options[i].name; i++) {
		/* find matching long option */
		if (strncmp(current_argv, long_options[i].name,
		    current_argv_len))
			continue;

		if (strlen(long_options[i].name) == current_argv_len) {
			/* exact match */
			match = i;
			exact_match = 1;
			break;
		}
		/*
		 * If this is a known short option, don't allow
		 * a partial match of a single character.
		 */
		if (short_too && current_argv_len == 1)
			continue;

		if (match == -1)	/* first partial match */
			match = i;
		else if ((flags & FLAG_LONGONLY) ||
			 long_options[i].has_arg !=
			     long_options[match].has_arg ||
			 long_options[i].flag != long_options[match].flag ||
			 long_options[i].val != long_options[match].val)
			second_partial_match = 1;
	}
	if (!exact_match && second_partial_match) {
		/* ambiguous abbreviation */
		if (PRINT_ERROR)
			warnx(ambig,
#ifdef GNU_COMPATIBLE
			     current_dash,
#endif
			     (int)current_argv_len,
			     current_argv);
		optopt_tls = 0;
		return (BADCH);
	}
	if (match != -1) {		/* option found */
		if (long_options[match].has_arg == no_argument
		    && has_equal) {
			if (PRINT_ERROR)
				warnx(noarg,
#ifdef GNU_COMPATIBLE
				     current_dash,
#endif
				     (int)current_argv_len,
				     current_argv);
			/*
			 * XXX: GNU sets optopt_tls to val regardless of flag
			 */
			if (long_options[match].flag == NULL)
				optopt_tls = long_options[match].val;
			else
				optopt_tls = 0;
#ifdef GNU_COMPATIBLE
			return (BADCH);
#else
			return (BADARG);
#endif
		}
		if (long_options[match].has_arg == required_argument ||
		    long_options[match].has_arg == optional_argument) {
			if (has_equal)
				optarg_tls = has_equal;
			else if (long_options[match].has_arg ==
			    required_argument) {
				/*
				 * optional argument doesn't use next nargv
				 */
				optarg_tls = nargv[optind_tls++];
			}
		}
		if ((long_options[match].has_arg == required_argument)
		    && (optarg_tls == NULL)) {
			/*
			 * Missing argument; leading ':' indicates no error
			 * should be generated.
			 */
			if (PRINT_ERROR)
				warnx(recargstring,
#ifdef GNU_COMPATIBLE
				    current_dash,
#endif
				    current_argv);
			/*
			 * XXX: GNU sets optopt_tls to val regardless of flag
			 */
			if (long_options[match].flag == NULL)
				optopt_tls = long_options[match].val;
			else
				optopt_tls = 0;
			--optind_tls;
			return (BADARG);
		}
	} else {			/* unknown option */
		if (short_too) {
			--optind_tls;
			return (-1);
		}
		if (PRINT_ERROR)
			warnx(illoptstring,
#ifdef GNU_COMPATIBLE
			      current_dash,
#endif
			      current_argv);
		optopt_tls = 0;
		return (BADCH);
	}
	if (idx)
		*idx = match;
	if (long_options[match].flag) {
		*long_options[match].flag = long_options[match].val;
		return (0);
	} else
		return (long_options[match].val);
}

/*
 * getopt_internal --
 *	Parse argc/argv argument vector.  Called by user level routines.
 *
 *	XC: Return thread-specific state in "goptsd" used by the caller across
 *	invocations (such as optarg and optind).
 */
static int
getopt_internal(int nargc, char * const *nargv, const char *options,
	const OptionThr *long_options, int *idx, int flags,
    GetOptDataThr *goptsd)
{
	char *oli;				/* option letter list index */
	int optchar, short_too;

	if (options == NULL)
		return (-1);

	/*
	 * XXX Some GNU programs (like cvs) set optind_tls to 0 instead of
	 * XXX using optreset_tls.  Work around this braindamage.
	 */
	if (optind_tls == 0)
		optind_tls = optreset_tls = 1;

	/*
	 * Disable GNU extensions if POSIXLY_CORRECT is set or options
	 * string begins with a '+'.
	 */
	if (posixly_correct == -1 || optreset_tls)
		posixly_correct = (getenv("POSIXLY_CORRECT") != NULL);
	if (*options == '-')
		flags |= FLAG_ALLARGS;
	else if (posixly_correct || *options == '+')
		flags &= ~FLAG_PERMUTE;
	if (*options == '+' || *options == '-')
		options++;

	optarg_tls = NULL;
	if (optreset_tls)
		nonopt_start = nonopt_end = -1;
start:
	if (optreset_tls || !*place) {		/* update scanning pointer */
		optreset_tls = 0;
		if (optind_tls >= nargc) {          /* end of argument vector */
			place = EMSG;
			if (nonopt_end != -1) {
				/* do permutation, if we have to */
				permute_args(nonopt_start, nonopt_end,
				    optind_tls, nargv);
				optind_tls -= nonopt_end - nonopt_start;
			}
			else if (nonopt_start != -1) {
				/*
				 * If we skipped non-options, set optind_tls
				 * to the first of them.
				 */
				optind_tls = nonopt_start;
			}
			nonopt_start = nonopt_end = -1;
			return (-1);
		}
		if (*(place = nargv[optind_tls]) != '-' ||
#ifdef GNU_COMPATIBLE
		    place[1] == '\0') {
#else
		    (place[1] == '\0' && strchr(options, '-') == NULL)) {
#endif
			place = EMSG;		/* found non-option */
			if (flags & FLAG_ALLARGS) {
				/*
				 * GNU extension:
				 * return non-option as argument to option 1
				 */
				optarg_tls = nargv[optind_tls++];
				return (INORDER);
			}
			if (!(flags & FLAG_PERMUTE)) {
				/*
				 * If no permutation wanted, stop parsing
				 * at first non-option.
				 */
				return (-1);
			}
			/* do permutation */
			if (nonopt_start == -1)
				nonopt_start = optind_tls;
			else if (nonopt_end != -1) {
				permute_args(nonopt_start, nonopt_end,
				    optind_tls, nargv);
				nonopt_start = optind_tls -
				    (nonopt_end - nonopt_start);
				nonopt_end = -1;
			}
			optind_tls++;
			/* process next argument */
			goto start;
		}
		if (nonopt_start != -1 && nonopt_end == -1)
			nonopt_end = optind_tls;

		/*
		 * If we have "-" do nothing, if "--" we are done.
		 */
		if (place[1] != '\0' && *++place == '-' && place[1] == '\0') {
			optind_tls++;
			place = EMSG;
			/*
			 * We found an option (--), so if we skipped
			 * non-options, we have to permute.
			 */
			if (nonopt_end != -1) {
				permute_args(nonopt_start, nonopt_end,
				    optind_tls, nargv);
				optind_tls -= nonopt_end - nonopt_start;
			}
			nonopt_start = nonopt_end = -1;
			return (-1);
		}
	}

	/*
	 * Check long options if:
	 *  1) we were passed some
	 *  2) the arg is not just "-"
	 *  3) either the arg starts with -- we are getopt_long_only()
	 */
	if (long_options != NULL && place != nargv[optind_tls] &&
	    (*place == '-' || (flags & FLAG_LONGONLY))) {
		short_too = 0;
#ifdef GNU_COMPATIBLE
		dash_prefix = D_PREFIX;
#endif
		if (*place == '-') {
			place++;		/* --foo long option */
#ifdef GNU_COMPATIBLE
			dash_prefix = DD_PREFIX;
#endif
		} else if (*place != ':' && strchr(options, *place) != NULL)
			short_too = 1;		/* could be short option too */

		optchar = parse_long_options(nargv, options, long_options,
		    idx, short_too, flags, goptsd);
		if (optchar != -1) {
			place = EMSG;
			return (optchar);
		}
	}

	if ((optchar = (int)*place++) == (int)':' ||
	    (optchar == (int)'-' && *place != '\0') ||
	    (oli = strchr(options, optchar)) == NULL) {
		/*
		 * If the user specified "-" and  '-' isn't listed in
		 * options, return -1 (non-option) as per POSIX.
		 * Otherwise, it is an unknown option character (or ':').
		 */
		if (optchar == (int)'-' && *place == '\0')
			return (-1);
		if (!*place)
			++optind_tls;
#ifdef GNU_COMPATIBLE
		if (PRINT_ERROR)
			warnx(posixly_correct ? illoptchar : gnuoptchar,
			      optchar);
#else
		if (PRINT_ERROR)
			warnx(illoptchar, optchar);
#endif
		optopt_tls = optchar;
		return (BADCH);
	}
	if (long_options != NULL && optchar == 'W' && oli[1] == ';') {
		/* -W long-option */
		if (*place)			/* no space */
			/* NOTHING */;
		else if (++optind_tls >= nargc) {	/* no arg */
			place = EMSG;
			if (PRINT_ERROR)
				warnx(recargchar, optchar);
			optopt_tls = optchar;
			return (BADARG);
		} else				/* white space */
			place = nargv[optind_tls];
#ifdef GNU_COMPATIBLE
		dash_prefix = W_PREFIX;
#endif
		optchar = parse_long_options(nargv, options, long_options,
		    idx, 0, flags, goptsd);
		place = EMSG;
		return (optchar);
	}
	if (*++oli != ':') {			/* doesn't take argument */
		if (!*place)
			++optind_tls;
	} else {				/* takes (optional) argument */
		optarg_tls = NULL;
		if (*place)			/* no white space */
			optarg_tls = place;
		else if (oli[1] != ':') {	/* arg not optional */
			if (++optind_tls >= nargc) {	/* no arg */
				place = EMSG;
				if (PRINT_ERROR)
					warnx(recargchar, optchar);
				optopt_tls = optchar;
				return (BADARG);
			} else
				optarg_tls = nargv[optind_tls];
		}
		place = EMSG;
		++optind_tls;
	}
	/* dump back option letter */
	return (optchar);
}

/*
 * XC:
 *
 * getOptLong() is typically called multiple times in a loop to parse a single
 * argc/argv pair. However, we'd like to enable the same thread to process
 * multiple argc/argv pairs in sequence: e.g. first to parse the args for an
 * index query, and then to parse the args for a filter query, etc.
 *
 * Since getOptLong() maintains internal, thread-local state across invocations,
 * to influence each invocation as it processes a single argc/argv pair, this
 * internal state must first be initialized before starting a new argc/argv
 * parsing sequence. The caller must know this rule, in order for a correct
 * calling sequence, an example of which is shown below:
 *
 * getOptLongInit();
   ...loop over calls to getOptLong(argc, argv, ...), say for index parsing ...

   getOptLongInit();
   ...loop over calls to getOptLong(argc, argv, ...), say for filter parsing ...

 * Such an interface is preferred, over the use of "optind" being set to 0 or 1,
 * as in the standard getopt(3) interface style - primarily because optind is
 * now a TLS variable, but also because this is cleaner.
 */

void
getOptLongInit()
{
    opterr_tls = 1;		/* if error message should be printed */
    optind_tls = 1;		/* index into parent argv vector */
    optopt_tls = '?';	/* character checked for validity */
    place = EMSG;       /* option letter processing */
    nonopt_start = -1;  /* first non option argument (for permute) */
    nonopt_end = -1;    /* first option after non options (for permute) */
    dash_prefix = NO_PREFIX;
    posixly_correct = -1;
}

/*
 * getopt_long --
 *	Parse argc/argv argument vector.
 *
 *	XC: Return thread-specific state in "goptsd" used by the caller across
 *	invocations (such as optarg and optind).
 */
int
getOptLong(int nargc, char * const *nargv, const char *options,
	const OptionThr *long_options, int *idx, GetOptDataThr *goptsd)
{

    int ret;

    ret = getopt_internal(nargc, nargv, options, long_options, idx,
	    FLAG_PERMUTE, goptsd);
    goptsd->optind = optind_tls;
    goptsd->optarg = optarg_tls;
    return ret;
}
