/*
 * test_fnv - FNV test suite
 *
 * @(#) $Revision: 5.3 $
 * @(#) $Id: test_fnv.c,v 5.3 2009/06/30 11:50:41 chongo Exp $
 * @(#) $Source: /usr/local/src/cmd/fnv/RCS/test_fnv.c,v $
 *
 ***
 *
 * Fowler/Noll/Vo hash
 *
 * The basis of this hash algorithm was taken from an idea sent
 * as reviewer comments to the IEEE POSIX P1003.2 committee by:
 *
 *      Phong Vo (http://www.research.att.com/info/kpv/)
 *      Glenn Fowler (http://www.research.att.com/~gsf/)
 *
 * In a subsequent ballot round:
 *
 *      Landon Curt Noll (http://www.isthe.com/chongo/)
 *
 * improved on their algorithm.  Some people tried this hash
 * and found that it worked rather well.  In an EMail message
 * to Landon, they named it the ``Fowler/Noll/Vo'' or FNV hash.
 *
 * FNV hashes are designed to be fast while maintaining a low
 * collision rate. The FNV speed allows one to quickly hash lots
 * of data while maintaining a reasonable collision rate.  See:
 *
 *      http://www.isthe.com/chongo/tech/comp/fnv/index.html
 *
 * for more details as well as other forms of the FNV hash.
 *
 ***
 *
 * Please do not copyright this code.  This code is in the public domain.
 *
 * LANDON CURT NOLL DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE,
 * INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO
 * EVENT SHALL LANDON CURT NOLL BE LIABLE FOR ANY SPECIAL, INDIRECT OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF
 * USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 *
 * By:
 *  chongo <Landon Curt Noll> /\oo/\
 *      http://www.isthe.com/chongo/
 *
 * Share and Enjoy! :-)
 */

#include <stdio.h>
#include <assert.h>

#include "primitives/Primitives.h"
#include "hash/Fnv.h"

#include "test/QA.h"

#define LEN(x) (sizeof(x)-1)
/* TEST macro does not include trailing NUL byte in the test vector */
#define TEST(x) {x, LEN(x)}
/* TEST0 macro includes the trailing NUL byte in the test vector */
#define TEST0(x) {x, sizeof(x)}
/* REPEAT500 - repeat a string 500 times */
#define R500(x) R100(x)R100(x)R100(x)R100(x)R100(x)
#define R100(x) R10(x)R10(x)R10(x)R10(x)R10(x)R10(x)R10(x)R10(x)R10(x)R10(x)
#define R10(x) x x x x x x x x x x

struct test_vector {
    const void *buf;      /* start of test vector buffer */
    int len;        /* length of test vector */
};

struct fnv0_64_test_vector {
    struct test_vector *test;   /* test vector buffer to hash */
    uint64_t fnv0_64;        /* expected FNV-0 64 bit hash value */
};

struct fnv1_64_test_vector {
    struct test_vector *test;   /* test vector buffer to hash */
    uint64_t fnv1_64;        /* expected FNV-1 64 bit hash value */
};

struct fnv1a_64_test_vector {
    struct test_vector *test;   /* test vector buffer to hash */
    uint64_t fnv1a_64;       /* expected FNV-1a 64 bit hash value */
};

/*
 * FNV test vectors
 *
 * NOTE: A NULL pointer marks beyond the end of the test vectors.
 *
 * NOTE: The order of the fnv_test_str[] test vectors is 1-to-1 with:
 *
 *  struct fnv0_64_test_vector fnv0_64_vector[];
 *  struct fnv1_64_test_vector fnv1_64_vector[];
 *  struct fnv1a_64_test_vector fnv1a_64_vector[];
 *
 * IMPORTANT NOTE:
 *
 *  If you change the fnv_test_str[] array, you need
 *  to also change ALL of the above fnv*_vector arrays!!!
 *
 *  and then fold the results into the source file.
 *  Of course, you better make sure that the vaules
 *  produced by the above command are valid, otherwise
 *  you will be testing against invalid vectors!
 */
struct test_vector fnv_test_str[] = {
  TEST(""),
  TEST("a"),
  TEST("b"),
  TEST("c"),
  TEST("d"),
  TEST("e"),
  TEST("f"),
  TEST("fo"),
  TEST("foo"),
  TEST("foob"),
  TEST("fooba"),
  TEST("foobar"),
  TEST0(""),
  TEST0("a"),
  TEST0("b"),
  TEST0("c"),
  TEST0("d"),
  TEST0("e"),
  TEST0("f"),
  TEST0("fo"),
  TEST0("foo"),
  TEST0("foob"),
  TEST0("fooba"),
  TEST0("foobar"),
  TEST("ch"),
  TEST("cho"),
  TEST("chon"),
  TEST("chong"),
  TEST("chongo"),
  TEST("chongo "),
  TEST("chongo w"),
  TEST("chongo wa"),
  TEST("chongo was"),
  TEST("chongo was "),
  TEST("chongo was h"),
  TEST("chongo was he"),
  TEST("chongo was her"),
  TEST("chongo was here"),
  TEST("chongo was here!"),
  TEST("chongo was here!\n"),
  TEST0("ch"),
  TEST0("cho"),
  TEST0("chon"),
  TEST0("chong"),
  TEST0("chongo"),
  TEST0("chongo "),
  TEST0("chongo w"),
  TEST0("chongo wa"),
  TEST0("chongo was"),
  TEST0("chongo was "),
  TEST0("chongo was h"),
  TEST0("chongo was he"),
  TEST0("chongo was her"),
  TEST0("chongo was here"),
  TEST0("chongo was here!"),
  TEST0("chongo was here!\n"),
  TEST("cu"),
  TEST("cur"),
  TEST("curd"),
  TEST("curds"),
  TEST("curds "),
  TEST("curds a"),
  TEST("curds an"),
  TEST("curds and"),
  TEST("curds and "),
  TEST("curds and w"),
  TEST("curds and wh"),
  TEST("curds and whe"),
  TEST("curds and whey"),
  TEST("curds and whey\n"),
  TEST0("cu"),
  TEST0("cur"),
  TEST0("curd"),
  TEST0("curds"),
  TEST0("curds "),
  TEST0("curds a"),
  TEST0("curds an"),
  TEST0("curds and"),
  TEST0("curds and "),
  TEST0("curds and w"),
  TEST0("curds and wh"),
  TEST0("curds and whe"),
  TEST0("curds and whey"),
  TEST0("curds and whey\n"),
  TEST("hi"), TEST0("hi"),
  TEST("hello"), TEST0("hello"),
  TEST("\xff\x00\x00\x01"), TEST("\x01\x00\x00\xff"),
  TEST("\xff\x00\x00\x02"), TEST("\x02\x00\x00\xff"),
  TEST("\xff\x00\x00\x03"), TEST("\x03\x00\x00\xff"),
  TEST("\xff\x00\x00\x04"), TEST("\x04\x00\x00\xff"),
  TEST("\x40\x51\x4e\x44"), TEST("\x44\x4e\x51\x40"),
  TEST("\x40\x51\x4e\x4a"), TEST("\x4a\x4e\x51\x40"),
  TEST("\x40\x51\x4e\x54"), TEST("\x54\x4e\x51\x40"),
  TEST("127.0.0.1"), TEST0("127.0.0.1"),
  TEST("127.0.0.2"), TEST0("127.0.0.2"),
  TEST("127.0.0.3"), TEST0("127.0.0.3"),
  TEST("64.81.78.68"), TEST0("64.81.78.68"),
  TEST("64.81.78.74"), TEST0("64.81.78.74"),
  TEST("64.81.78.84"), TEST0("64.81.78.84"),
  TEST("feedface"), TEST0("feedface"),
  TEST("feedfacedaffdeed"), TEST0("feedfacedaffdeed"),
  TEST("feedfacedeadbeef"), TEST0("feedfacedeadbeef"),
  TEST("line 1\nline 2\nline 3"),
  TEST("chongo <Landon Curt Noll> /\\../\\"),
  TEST0("chongo <Landon Curt Noll> /\\../\\"),
  TEST("chongo (Landon Curt Noll) /\\../\\"),
  TEST0("chongo (Landon Curt Noll) /\\../\\"),
  TEST("http://antwrp.gsfc.nasa.gov/apod/astropix.html"),
  TEST("http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash"),
  TEST("http://epod.usra.edu/"),
  TEST("http://exoplanet.eu/"),
  TEST("http://hvo.wr.usgs.gov/cam3/"),
  TEST("http://hvo.wr.usgs.gov/cams/HMcam/"),
  TEST("http://hvo.wr.usgs.gov/kilauea/update/deformation.html"),
  TEST("http://hvo.wr.usgs.gov/kilauea/update/images.html"),
  TEST("http://hvo.wr.usgs.gov/kilauea/update/maps.html"),
  TEST("http://hvo.wr.usgs.gov/volcanowatch/current_issue.html"),
  TEST("http://neo.jpl.nasa.gov/risk/"),
  TEST("http://norvig.com/21-days.html"),
  TEST("http://primes.utm.edu/curios/home.php"),
  TEST("http://slashdot.org/"),
  TEST("http://tux.wr.usgs.gov/Maps/155.25-19.5.html"),
  TEST("http://volcano.wr.usgs.gov/kilaueastatus.php"),
  TEST("http://www.avo.alaska.edu/activity/Redoubt.php"),
  TEST("http://www.dilbert.com/fast/"),
  TEST("http://www.fourmilab.ch/gravitation/orbits/"),
  TEST("http://www.fpoa.net/"),
  TEST("http://www.ioccc.org/index.html"),
  TEST("http://www.isthe.com/cgi-bin/number.cgi"),
  TEST("http://www.isthe.com/chongo/bio.html"),
  TEST("http://www.isthe.com/chongo/index.html"),
  TEST("http://www.isthe.com/chongo/src/calc/lucas-calc"),
  TEST("http://www.isthe.com/chongo/tech/astro/venus2004.html"),
  TEST("http://www.isthe.com/chongo/tech/astro/vita.html"),
  TEST("http://www.isthe.com/chongo/tech/comp/c/expert.html"),
  TEST("http://www.isthe.com/chongo/tech/comp/calc/index.html"),
  TEST("http://www.isthe.com/chongo/tech/comp/fnv/index.html"),
  TEST("http://www.isthe.com/chongo/tech/math/number/howhigh.html"),
  TEST("http://www.isthe.com/chongo/tech/math/number/number.html"),
  TEST("http://www.isthe.com/chongo/tech/math/prime/mersenne.html"),
  TEST("http://www.isthe.com/chongo/tech/math/prime/mersenne.html#largest"),
  TEST("http://www.lavarnd.org/cgi-bin/corpspeak.cgi"),
  TEST("http://www.lavarnd.org/cgi-bin/haiku.cgi"),
  TEST("http://www.lavarnd.org/cgi-bin/rand-none.cgi"),
  TEST("http://www.lavarnd.org/cgi-bin/randdist.cgi"),
  TEST("http://www.lavarnd.org/index.html"),
  TEST("http://www.lavarnd.org/what/nist-test.html"),
  TEST("http://www.macosxhints.com/"),
  TEST("http://www.mellis.com/"),
  TEST("http://www.nature.nps.gov/air/webcams/parks/havoso2alert/"
       "havoalert.cfm"),
  TEST("http://www.nature.nps.gov/air/webcams/parks/havoso2alert/"
       "timelines_24.cfm"),
  TEST("http://www.paulnoll.com/"),
  TEST("http://www.pepysdiary.com/"),
  TEST("http://www.sciencenews.org/index/home/activity/view"),
  TEST("http://www.skyandtelescope.com/"),
  TEST("http://www.sput.nl/~rob/sirius.html"),
  TEST("http://www.systemexperts.com/"),
  TEST("http://www.tq-international.com/phpBB3/index.php"),
  TEST("http://www.travelquesttours.com/index.htm"),
  TEST("http://www.wunderground.com/global/stations/89606.html"),
  TEST(R10("21701")),
  TEST(R10("M21701")),
  TEST(R10("2^21701-1")),
  TEST(R10("\x54\xc5")),
  TEST(R10("\xc5\x54")),
  TEST(R10("23209")),
  TEST(R10("M23209")),
  TEST(R10("2^23209-1")),
  TEST(R10("\x5a\xa9")),
  TEST(R10("\xa9\x5a")),
  TEST(R10("391581216093")),
  TEST(R10("391581*2^216093-1")),
  TEST(R10("\x05\xf9\x9d\x03\x4c\x81")),
  TEST(R10("FEDCBA9876543210")),
  TEST(R10("\xfe\xdc\xba\x98\x76\x54\x32\x10")),
  TEST(R10("EFCDAB8967452301")),
  TEST(R10("\xef\xcd\xab\x89\x67\x45\x23\x01")),
  TEST(R10("0123456789ABCDEF")),
  TEST(R10("\x01\x23\x45\x67\x89\xab\xcd\xef")),
  TEST(R10("1032547698BADCFE")),
  TEST(R10("\x10\x32\x54\x76\x98\xba\xdc\xfe")),
  TEST(R500("\x00")),
  TEST(R500("\x07")),
  TEST(R500("~")),
  TEST(R500("\x7f")),
};

/* FNV-1a 64 bit test vectors */
struct fnv1a_64_test_vector fnv1a_64_vector[] = {
    { &fnv_test_str[0], 0xcbf29ce484222325ULL },
    { &fnv_test_str[1], 0xaf63dc4c8601ec8cULL },
    { &fnv_test_str[2], 0xaf63df4c8601f1a5ULL },
    { &fnv_test_str[3], 0xaf63de4c8601eff2ULL },
    { &fnv_test_str[4], 0xaf63d94c8601e773ULL },
    { &fnv_test_str[5], 0xaf63d84c8601e5c0ULL },
    { &fnv_test_str[6], 0xaf63db4c8601ead9ULL },
    { &fnv_test_str[7], 0x08985907b541d342ULL },
    { &fnv_test_str[8], 0xdcb27518fed9d577ULL },
    { &fnv_test_str[9], 0xdd120e790c2512afULL },
    { &fnv_test_str[10], 0xcac165afa2fef40aULL },
    { &fnv_test_str[11], 0x85944171f73967e8ULL },
    { &fnv_test_str[12], 0xaf63bd4c8601b7dfULL },
    { &fnv_test_str[13], 0x089be207b544f1e4ULL },
    { &fnv_test_str[14], 0x08a61407b54d9b5fULL },
    { &fnv_test_str[15], 0x08a2ae07b54ab836ULL },
    { &fnv_test_str[16], 0x0891b007b53c4869ULL },
    { &fnv_test_str[17], 0x088e4a07b5396540ULL },
    { &fnv_test_str[18], 0x08987c07b5420ebbULL },
    { &fnv_test_str[19], 0xdcb28a18fed9f926ULL },
    { &fnv_test_str[20], 0xdd1270790c25b935ULL },
    { &fnv_test_str[21], 0xcac146afa2febf5dULL },
    { &fnv_test_str[22], 0x8593d371f738acfeULL },
    { &fnv_test_str[23], 0x34531ca7168b8f38ULL },
    { &fnv_test_str[24], 0x08a25607b54a22aeULL },
    { &fnv_test_str[25], 0xf5faf0190cf90df3ULL },
    { &fnv_test_str[26], 0xf27397910b3221c7ULL },
    { &fnv_test_str[27], 0x2c8c2b76062f22e0ULL },
    { &fnv_test_str[28], 0xe150688c8217b8fdULL },
    { &fnv_test_str[29], 0xf35a83c10e4f1f87ULL },
    { &fnv_test_str[30], 0xd1edd10b507344d0ULL },
    { &fnv_test_str[31], 0x2a5ee739b3ddb8c3ULL },
    { &fnv_test_str[32], 0xdcfb970ca1c0d310ULL },
    { &fnv_test_str[33], 0x4054da76daa6da90ULL },
    { &fnv_test_str[34], 0xf70a2ff589861368ULL },
    { &fnv_test_str[35], 0x4c628b38aed25f17ULL },
    { &fnv_test_str[36], 0x9dd1f6510f78189fULL },
    { &fnv_test_str[37], 0xa3de85bd491270ceULL },
    { &fnv_test_str[38], 0x858e2fa32a55e61dULL },
    { &fnv_test_str[39], 0x46810940eff5f915ULL },
    { &fnv_test_str[40], 0xf5fadd190cf8edaaULL },
    { &fnv_test_str[41], 0xf273ed910b32b3e9ULL },
    { &fnv_test_str[42], 0x2c8c5276062f6525ULL },
    { &fnv_test_str[43], 0xe150b98c821842a0ULL },
    { &fnv_test_str[44], 0xf35aa3c10e4f55e7ULL },
    { &fnv_test_str[45], 0xd1ed680b50729265ULL },
    { &fnv_test_str[46], 0x2a5f0639b3dded70ULL },
    { &fnv_test_str[47], 0xdcfbaa0ca1c0f359ULL },
    { &fnv_test_str[48], 0x4054ba76daa6a430ULL },
    { &fnv_test_str[49], 0xf709c7f5898562b0ULL },
    { &fnv_test_str[50], 0x4c62e638aed2f9b8ULL },
    { &fnv_test_str[51], 0x9dd1a8510f779415ULL },
    { &fnv_test_str[52], 0xa3de2abd4911d62dULL },
    { &fnv_test_str[53], 0x858e0ea32a55ae0aULL },
    { &fnv_test_str[54], 0x46810f40eff60347ULL },
    { &fnv_test_str[55], 0xc33bce57bef63eafULL },
    { &fnv_test_str[56], 0x08a24307b54a0265ULL },
    { &fnv_test_str[57], 0xf5b9fd190cc18d15ULL },
    { &fnv_test_str[58], 0x4c968290ace35703ULL },
    { &fnv_test_str[59], 0x07174bd5c64d9350ULL },
    { &fnv_test_str[60], 0x5a294c3ff5d18750ULL },
    { &fnv_test_str[61], 0x05b3c1aeb308b843ULL },
    { &fnv_test_str[62], 0xb92a48da37d0f477ULL },
    { &fnv_test_str[63], 0x73cdddccd80ebc49ULL },
    { &fnv_test_str[64], 0xd58c4c13210a266bULL },
    { &fnv_test_str[65], 0xe78b6081243ec194ULL },
    { &fnv_test_str[66], 0xb096f77096a39f34ULL },
    { &fnv_test_str[67], 0xb425c54ff807b6a3ULL },
    { &fnv_test_str[68], 0x23e520e2751bb46eULL },
    { &fnv_test_str[69], 0x1a0b44ccfe1385ecULL },
    { &fnv_test_str[70], 0xf5ba4b190cc2119fULL },
    { &fnv_test_str[71], 0x4c962690ace2baafULL },
    { &fnv_test_str[72], 0x0716ded5c64cda19ULL },
    { &fnv_test_str[73], 0x5a292c3ff5d150f0ULL },
    { &fnv_test_str[74], 0x05b3e0aeb308ecf0ULL },
    { &fnv_test_str[75], 0xb92a5eda37d119d9ULL },
    { &fnv_test_str[76], 0x73ce41ccd80f6635ULL },
    { &fnv_test_str[77], 0xd58c2c132109f00bULL },
    { &fnv_test_str[78], 0xe78baf81243f47d1ULL },
    { &fnv_test_str[79], 0xb0968f7096a2ee7cULL },
    { &fnv_test_str[80], 0xb425a84ff807855cULL },
    { &fnv_test_str[81], 0x23e4e9e2751b56f9ULL },
    { &fnv_test_str[82], 0x1a0b4eccfe1396eaULL },
    { &fnv_test_str[83], 0x54abd453bb2c9004ULL },
    { &fnv_test_str[84], 0x08ba5f07b55ec3daULL },
    { &fnv_test_str[85], 0x337354193006cb6eULL },
    { &fnv_test_str[86], 0xa430d84680aabd0bULL },
    { &fnv_test_str[87], 0xa9bc8acca21f39b1ULL },
    { &fnv_test_str[88], 0x6961196491cc682dULL },
    { &fnv_test_str[89], 0xad2bb1774799dfe9ULL },
    { &fnv_test_str[90], 0x6961166491cc6314ULL },
    { &fnv_test_str[91], 0x8d1bb3904a3b1236ULL },
    { &fnv_test_str[92], 0x6961176491cc64c7ULL },
    { &fnv_test_str[93], 0xed205d87f40434c7ULL },
    { &fnv_test_str[94], 0x6961146491cc5faeULL },
    { &fnv_test_str[95], 0xcd3baf5e44f8ad9cULL },
    { &fnv_test_str[96], 0xe3b36596127cd6d8ULL },
    { &fnv_test_str[97], 0xf77f1072c8e8a646ULL },
    { &fnv_test_str[98], 0xe3b36396127cd372ULL },
    { &fnv_test_str[99], 0x6067dce9932ad458ULL },
    { &fnv_test_str[100], 0xe3b37596127cf208ULL },
    { &fnv_test_str[101], 0x4b7b10fa9fe83936ULL },
    { &fnv_test_str[102], 0xaabafe7104d914beULL },
    { &fnv_test_str[103], 0xf4d3180b3cde3edaULL },
    { &fnv_test_str[104], 0xaabafd7104d9130bULL },
    { &fnv_test_str[105], 0xf4cfb20b3cdb5bb1ULL },
    { &fnv_test_str[106], 0xaabafc7104d91158ULL },
    { &fnv_test_str[107], 0xf4cc4c0b3cd87888ULL },
    { &fnv_test_str[108], 0xe729bac5d2a8d3a7ULL },
    { &fnv_test_str[109], 0x74bc0524f4dfa4c5ULL },
    { &fnv_test_str[110], 0xe72630c5d2a5b352ULL },
    { &fnv_test_str[111], 0x6b983224ef8fb456ULL },
    { &fnv_test_str[112], 0xe73042c5d2ae266dULL },
    { &fnv_test_str[113], 0x8527e324fdeb4b37ULL },
    { &fnv_test_str[114], 0x0a83c86fee952abcULL },
    { &fnv_test_str[115], 0x7318523267779d74ULL },
    { &fnv_test_str[116], 0x3e66d3d56b8caca1ULL },
    { &fnv_test_str[117], 0x956694a5c0095593ULL },
    { &fnv_test_str[118], 0xcac54572bb1a6fc8ULL },
    { &fnv_test_str[119], 0xa7a4c9f3edebf0d8ULL },
    { &fnv_test_str[120], 0x7829851fac17b143ULL },
    { &fnv_test_str[121], 0x2c8f4c9af81bcf06ULL },
    { &fnv_test_str[122], 0xd34e31539740c732ULL },
    { &fnv_test_str[123], 0x3605a2ac253d2db1ULL },
    { &fnv_test_str[124], 0x08c11b8346f4a3c3ULL },
    { &fnv_test_str[125], 0x6be396289ce8a6daULL },
    { &fnv_test_str[126], 0xd9b957fb7fe794c5ULL },
    { &fnv_test_str[127], 0x05be33da04560a93ULL },
    { &fnv_test_str[128], 0x0957f1577ba9747cULL },
    { &fnv_test_str[129], 0xda2cc3acc24fba57ULL },
    { &fnv_test_str[130], 0x74136f185b29e7f0ULL },
    { &fnv_test_str[131], 0xb2f2b4590edb93b2ULL },
    { &fnv_test_str[132], 0xb3608fce8b86ae04ULL },
    { &fnv_test_str[133], 0x4a3a865079359063ULL },
    { &fnv_test_str[134], 0x5b3a7ef496880a50ULL },
    { &fnv_test_str[135], 0x48fae3163854c23bULL },
    { &fnv_test_str[136], 0x07aaa640476e0b9aULL },
    { &fnv_test_str[137], 0x2f653656383a687dULL },
    { &fnv_test_str[138], 0xa1031f8e7599d79cULL },
    { &fnv_test_str[139], 0xa31908178ff92477ULL },
    { &fnv_test_str[140], 0x097edf3c14c3fb83ULL },
    { &fnv_test_str[141], 0xb51ca83feaa0971bULL },
    { &fnv_test_str[142], 0xdd3c0d96d784f2e9ULL },
    { &fnv_test_str[143], 0x86cd26a9ea767d78ULL },
    { &fnv_test_str[144], 0xe6b215ff54a30c18ULL },
    { &fnv_test_str[145], 0xec5b06a1c5531093ULL },
    { &fnv_test_str[146], 0x45665a929f9ec5e5ULL },
    { &fnv_test_str[147], 0x8c7609b4a9f10907ULL },
    { &fnv_test_str[148], 0x89aac3a491f0d729ULL },
    { &fnv_test_str[149], 0x32ce6b26e0f4a403ULL },
    { &fnv_test_str[150], 0x614ab44e02b53e01ULL },
    { &fnv_test_str[151], 0xfa6472eb6eef3290ULL },
    { &fnv_test_str[152], 0x9e5d75eb1948eb6aULL },
    { &fnv_test_str[153], 0xb6d12ad4a8671852ULL },
    { &fnv_test_str[154], 0x88826f56eba07af1ULL },
    { &fnv_test_str[155], 0x44535bf2645bc0fdULL },
    { &fnv_test_str[156], 0x169388ffc21e3728ULL },
    { &fnv_test_str[157], 0xf68aac9e396d8224ULL },
    { &fnv_test_str[158], 0x8e87d7e7472b3883ULL },
    { &fnv_test_str[159], 0x295c26caa8b423deULL },
    { &fnv_test_str[160], 0x322c814292e72176ULL },
    { &fnv_test_str[161], 0x8a06550eb8af7268ULL },
    { &fnv_test_str[162], 0xef86d60e661bcf71ULL },
    { &fnv_test_str[163], 0x9e5426c87f30ee54ULL },
    { &fnv_test_str[164], 0xf1ea8aa826fd047eULL },
    { &fnv_test_str[165], 0x0babaf9a642cb769ULL },
    { &fnv_test_str[166], 0x4b3341d4068d012eULL },
    { &fnv_test_str[167], 0xd15605cbc30a335cULL },
    { &fnv_test_str[168], 0x5b21060aed8412e5ULL },
    { &fnv_test_str[169], 0x45e2cda1ce6f4227ULL },
    { &fnv_test_str[170], 0x50ae3745033ad7d4ULL },
    { &fnv_test_str[171], 0xaa4588ced46bf414ULL },
    { &fnv_test_str[172], 0xc1b0056c4a95467eULL },
    { &fnv_test_str[173], 0x56576a71de8b4089ULL },
    { &fnv_test_str[174], 0xbf20965fa6dc927eULL },
    { &fnv_test_str[175], 0x569f8383c2040882ULL },
    { &fnv_test_str[176], 0xe1e772fba08feca0ULL },
    { &fnv_test_str[177], 0x4ced94af97138ac4ULL },
    { &fnv_test_str[178], 0xc4112ffb337a82fbULL },
    { &fnv_test_str[179], 0xd64a4fd41de38b7dULL },
    { &fnv_test_str[180], 0x4cfc32329edebcbbULL },
    { &fnv_test_str[181], 0x0803564445050395ULL },
    { &fnv_test_str[182], 0xaa1574ecf4642ffdULL },
    { &fnv_test_str[183], 0x694bc4e54cc315f9ULL },
    { &fnv_test_str[184], 0xa3d7cb273b011721ULL },
    { &fnv_test_str[185], 0x577c2f8b6115bfa5ULL },
    { &fnv_test_str[186], 0xb7ec8c1a769fb4c1ULL },
    { &fnv_test_str[187], 0x5d5cfce63359ab19ULL },
    { &fnv_test_str[188], 0x33b96c3cd65b5f71ULL },
    { &fnv_test_str[189], 0xd845097780602bb9ULL },
    { &fnv_test_str[190], 0x84d47645d02da3d5ULL },
    { &fnv_test_str[191], 0x83544f33b58773a5ULL },
    { &fnv_test_str[192], 0x9175cbb2160836c5ULL },
    { &fnv_test_str[193], 0xc71b3bc175e72bc5ULL },
    { &fnv_test_str[194], 0x636806ac222ec985ULL },
    { &fnv_test_str[195], 0xb6ef0e6950f52ed5ULL },
    { &fnv_test_str[196], 0xead3d8a0f3dfdaa5ULL },
    { &fnv_test_str[197], 0x922908fe9a861ba5ULL },
    { &fnv_test_str[198], 0x6d4821de275fd5c5ULL },
    { &fnv_test_str[199], 0x1fe3fce62bd816b5ULL },
    { &fnv_test_str[200], 0xc23e9fccd6f70591ULL },
    { &fnv_test_str[201], 0xc1af12bdfe16b5b5ULL },
    { &fnv_test_str[202], 0x39e9f18f2f85e221ULL },
};

void
fnvTests(void)
{
    unsigned ii;
    uint64_t hval;

    assertStatic(ArrayLen(fnv_test_str) == ArrayLen(fnv1a_64_vector));

    for (ii = 0; ii < ArrayLen(fnv_test_str); ii++) {
        hval = fnv_64a_buf(fnv_test_str[ii].buf, fnv_test_str[ii].len,
                           FNV1A_64_INIT);
        assert(hval == fnv1a_64_vector[ii].fnv1a_64);
    }
}
