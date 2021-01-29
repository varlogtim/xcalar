// strlcpy and strlcat implementations derived from
//   http://www.openbsd.org/cgi-bin/cvsweb/src/lib/libc/string/
/*
 * Copyright (c) 1998 Todd C. Miller <Todd.Miller@courtesan.com>
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
 */

// strnstr implementation derived from
//   https://svnweb.freebsd.org/base/head/lib/libc/string/strnstr.c
/*
 * Copyright (c) 2001 Mike Barcroft <mike@FreeBSD.org>
 * Copyright (c) 1990, 1993
 *      The Regents of the University of California.  All rights reserved.
 *
 * This code is derived from software contributed to Berkeley by
 * Chris Torek.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "StrlFunc.h"
#include <string.h>

size_t strlcpy(char *dst, const char *src, size_t size) {
    const size_t srcSize = strlen(src);
    size_t copied = size - 1;

    char *d = dst;
    char *s = (char *) src;
    char val = '\1';

    if (size != 0 && srcSize > 0 && copied > 0) {
        do {
            val = *d++ = *s++;

            if (val == '\0') {
                break;
            }

            copied--;
        } while (copied != 0);
    }

    if (size != 0 && val != '\0') {
        *d = '\0';
    }

    return (srcSize);
}

size_t strlcat(char *dst, const char *src, size_t size) {
    const size_t srcSize = strlen(src);
    const size_t dstSize = strlen(dst);
    size_t copied = size - dstSize - 1;

    char *d = dst + dstSize;
    char *s = (char *) src;
    char val = '\1';

    if (size != 0 && srcSize > 0 && copied > 0) {
        do {
            val = *d++ = *s++;

            if (val == '\0') {
                break;
            }

            copied--;
        } while (copied != 0);
    }

    if (size != 0 && val != '\0') {
        *d = '\0';
    }

    return (srcSize + dstSize);
}

char * strnstr(const char *big, const char *little, size_t len) {
  char next, subNext;
  size_t subLen;

  next = *little++;
  if (next != '\0') {
    subLen = strlen(little);

    do {
      do {
        subNext = *big++;
        if (subNext == '\0' || len-- < 1) {
          return (NULL);
        }
      } while (subNext != next);

      if (subLen > len) {
       return (NULL);
      }
    } while (strncmp(big, little, subLen) != 0);
    big--;
  }
  return ((char *) big);
}
