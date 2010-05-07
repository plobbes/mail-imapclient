#!/usr/bin/perl

use strict;
use warnings;
use Test::More tests => 18;

BEGIN { use_ok('Mail::IMAPClient::BodyStructure') or exit; }

my $bs = <<'END_OF_BS';
(BODYSTRUCTURE ("TEXT" "PLAIN" ("CHARSET" "us-ascii") NIL NIL "7BIT" 511 20 NIL NIL NIL))^M
END_OF_BS

my $bsobj = Mail::IMAPClient::BodyStructure->new($bs);
ok( defined $bsobj, 'parsed first' );
is( $bsobj->bodytype,    'TEXT',  'bodytype' );
is( $bsobj->bodysubtype, 'PLAIN', 'bodysubtype' );

my $bs2 = <<'END_OF_BS2';
(BODYSTRUCTURE (("TEXT" "PLAIN" ("CHARSET" 'us-ascii') NIL NIL "7BIT" 2 1 NIL NIL NIL)("MESSAGE" "RFC822" NIL NIL NIL "7BIT" 3930 ("Tue, 16 Jul 2002 15:29:17 -0400" "Re: [Fwd: Here is the the list of uids]" (("Michael Etcetera" NIL "michael.etcetera" "generic.com")) (("Michael Etcetera" NIL "michael.etcetera" "generic.com")) (("Michael Etcetera" NIL "michael.etcetera" "generic.com")) (("Michael Etcetera" NIL "michael.etcetera" "generic.com")) (("David J Kavid" NIL "david.kavid" "generic.com")) NIL NIL "<72f9a217.a21772f9@generic.com>") (("TEXT" "PLAIN" ("CHARSET" "us-ascii") NIL NIL "7BIT" 369 11 NIL NIL NIL)("MESSAGE" "RFC822" NIL NIL NIL "7BIT" 2599 ("Tue, 9 Jul 2002 13:42:04 -0400" "Here is the the list of uids" (("Nicholas Kringle" NIL "nicholas.kringle" "generic.com")) (("Nicholas Kringle" NIL "nicholas.kringle" "generic.com")) (("Nicholas Kringle" NIL "nicholas.kringle" "generic.com")) (("Michael Etcetera" NIL "michael.etcetera" "generic.com")) (("Richard W Continued" NIL "richard.continued" "generic.com")) NIL NIL "<015401c2276f$f09b7c10$59cab08c@one.two.generic.com>") ((("TEXT" "PLAIN" ("CHARSET" "iso-8859-1") NIL NIL "QUOTED-PRINTABLE" 256 10 NIL NIL NIL)("TEXT" "HTML" ("CHARSET" "iso-8859-1") NIL NIL "QUOTED-PRINTABLE" 791 22 NIL NIL NIL) "ALTERNATIVE" ("BOUNDARY" "----=_NextPart_001_0151_01C2274E.6969D0F0") NIL NIL) "MIXED" ("BOUNDARY" "----=_NextPart_000_0150_01C2274E.6969D0F0") NIL NIL) 75 NIL NIL NIL) "MIXED" ("BOUNDARY" "--1f34eac2082b02") NIL ("EN")) 118 NIL NIL NIL) "MIXED" ("BOUNDARY" "------------F600BD8FDDD648ABA72A09E0") NIL NIL))
END_OF_BS2

$bsobj = Mail::IMAPClient::BodyStructure->new($bs2);
ok( defined $bsobj, 'parsed second' );
is( $bsobj->bodytype,    'MULTIPART', 'bodytype' );
is( $bsobj->bodysubtype, 'MIXED',     'bodysubtype' );

is(
    join( "#", $bsobj->parts ),

    # Parsing in version 3.03, changed outcome from
    #  this: "1#2#2.HEAD#2.1#2.2#2.2.HEAD#2.2.1#2.2.1.1#2.2.1.2"
    #  to:   "1#2#2.HEAD#2.1#2.1.1#2.1.2#2.1.2.HEAD#2.1.2.1#2.1.2.1.1#2.1.2.1.1.1#2.1.2.1.1.2"
    # Patches to BodyStructure.pm in 3.24 changed it to this:
    "1#2#2.HEAD#2.TEXT#2.1#2.2#2.2.HEAD#2.2.TEXT#2.2.1#2.2.1#2.2.2",
    'parts'
);

my $bs3 = <<'END_OF_BS3';
FETCH (UID 1 BODYSTRUCTURE (("TEXT" "PLAIN" ("charset" "ISO-8859-1")
NIL NIL "quoted-printable" 1744 0)("TEXT" "HTML" ("charset"
"ISO-8859-1") NIL NIL "quoted-printable" 1967 0) "ALTERNATIVE"))
END_OF_BS3

$bsobj = Mail::IMAPClient::BodyStructure->new($bs3);
ok( defined $bsobj, 'parsed third' );

my $bs4 = <<'END_OF_BS4';
* 9 FETCH (UID 9 BODYSTRUCTURE (("TEXT" "PLAIN" ("charset" "us-ascii") NIL "Notification" "7BIT" 588 0)("MESSAGE" "DELIVERY-STATUS" NIL NIL "Delivery report" "7BIT" 459)("MESSAGE" "RFC822" NIL NIL "Undelivered Message" "8bit" 10286 ("Thu, 31 May 2007 11:25:56 +0200 (CEST)" "*****SPAM***** RE: Daily News" (("admin@activtrades.com" NIL "polettld" "ensma.fr")) (("admin@activtrades.com" NIL "polettld" "ensma.fr")) (("admin@activtrades.com" NIL "polettld" "ensma.fr")) ((NIL NIL "polettld" "ensma.fr")) NIL NIL "NIL" "<20070531133257.92825.qmail@cc299962-a.haaks1.ov.home.nl>") (("TEXT" "PLAIN" ("charset" "iso-8859-1") NIL NIL "7bit" 1510 0)("MESSAGE" "RFC822" ("name" "message" "x-spam-type" "original") NIL "Original message" "8bit" 5718) "MIXED")) "REPORT"))
END_OF_BS4

$bsobj = Mail::IMAPClient::BodyStructure->new($bs4);
ok( defined $bsobj, 'parsed fourth' );

# test bodyMD5, contributed by Micheal Stok
my $bs5 = <<'END_OF_BS5';
* 6 FETCH (UID 17280 BODYSTRUCTURE ((("text" "plain" ("charset" "utf-8") NIL NIL "quoted-printable" 1143 37 NIL NIL NIL)("text" "html" ("charset" "utf-8") NIL NIL "quoted-printable" 4618 106 NIL NIL NIL) "alternative" ("boundary" "Boundary-00=_Z7P340MWKGMMYJ0CCJD0") NIL NIL)("image" "tiff" ("name" "8dd0e430.tif") NIL NIL "base64" 204134 "pmZp5QOBa9BIqFNmvxUiyQ==" ("attachment" ("filename" "8dd0e430.tif")) NIL) "mixed" ("boundary" "Boundary-00=_T7P340MWKGMMYJ0CCJD0") NIL NIL))
END_OF_BS5

my @exp;
$bsobj = Mail::IMAPClient::BodyStructure->new($bs5);
@exp = qw(1 1.1 1.2 2);
ok( defined $bsobj, 'parsed fifth' );
is_deeply( [ $bsobj->parts ], \@exp, 'bs5 parts' )
  or diag( join(" ", $bsobj->parts ) );

#
my $bs6 = q{(BODYSTRUCTURE (("text" "plain" ("charset" "UTF-8" "format" "flowed") NIL NIL "8bit" 82 6 NIL NIL NIL NIL)("message" "rfc822" ("name" "this is internal letter.eml") NIL NIL "7bit" 243436 ("Mon, 24 Aug 2009 10:51:22 +0400" "this is internal letter" ((NIL NIL "icestar" "inbox.ru")) ((NIL NIL "icestar" "inbox.ru")) ((NIL NIL "icestar" "inbox.ru")) ((NIL NIL "dima" "adriver.ru")) NIL NIL NIL "<4A92386A.9080307@inbox.ru>") (("text" "plain" ("charset" "UTF-8" "format" "flowed") NIL NIL "7bit" 116 7 NIL NIL NIL NIL)("text" "xml" ("name" "mediaplan.xml" "charset" "us-ascii") NIL NIL "base64" 31412 424 NIL ("inline" ("filename" "mediaplan.xml")) NIL NIL)("application" "zip" ("name" "banners2.zip") NIL NIL "base64" 209942 NIL ("inline" ("filename" "banners2.zip")) NIL NIL) "mixed" ("boundary" "------------070804080502030807020509") NIL NIL NIL) 3326 NIL ("inline" ("filename" "this is internal letter.eml")) NIL NIL) "mixed" ("boundary" "------------070704030806000803040203") NIL NIL NIL))};

$bsobj = Mail::IMAPClient::BodyStructure->new($bs6);
@exp   = qw(1 2 2.HEAD 2.TEXT 2.1 2.2 2.3);
ok( defined $bsobj, 'parsed sixth' );
is_deeply( [ $bsobj->parts ], \@exp, 'bs6 parts' )
  or diag( join(" ", $bsobj->parts ) );

#
my $bs7 = q{(BODYSTRUCTURE (("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 20 1 NIL NIL NIL NIL)("message" "rfc822" NIL NIL NIL "7bit" 1810 ("Fri,07 May 2010 01:55:07 -0400" "wrap inner a" (("Phil Pearl" NIL "phil" "perkpartners.com")) (("Phil Pearl" NIL "phil" "perkpartners.com")) (("Phil Pearl" NIL "phil" "perkpartners.com")) ((NIL NIL "phil" "perkpartners.com")) NIL NIL NIL "<25015.1273211707@local>") (("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 27 3 NIL NIL NIL NIL)("message" "rfc822" NIL NIL NIL "7bit" 783 ("Fri, 07 May 2010 01:54:14 -0400" "inner msg #1" (("Phil Pearl" NIL "phil" "perkpartners.com")) (("Phil Pearl" NIL "phil" "perkpartners.com")) (("Phil Pearl" NIL "phil" "perkpartners.com")) ((NIL NIL "phil" "perkpartners.com")) NIL NIL NIL "<24986.1273211654@local>") ("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 25 3 NIL NIL NIL NIL) 23 NIL ("inline" ("filename" "52")) NIL NIL) "mixed" ("boundary" "=-=-=") NIL NIL NIL) 58 NIL ("inline" ("filename" "53")) NIL NIL) "mixed" ("boundary""==-=-=") NIL NIL NIL))};

$bsobj = Mail::IMAPClient::BodyStructure->new($bs7);
@exp   = qw(1 2 2.HEAD 2.TEXT 2.1 2.2 2.2.HEAD 2.2.TEXT);
ok( defined $bsobj, 'parsed seventh' );
is_deeply( [ $bsobj->parts ], \@exp, 'bs7 parts' )
  or diag( join(" ", $bsobj->parts ) );

#
my $bs8 = q{(BODYSTRUCTURE (("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 31 2 NIL NIL NIL NIL)("message" "rfc822" NIL NIL "My forwarded message" "7bit" 2833 ("Fri, 07 May 2010 01:55:40 -0400" "outer msg" (("Phil Pearl" NIL "phil" "perkpartners.com")) (("Phil Pearl" NIL "phil" "perkpartners.com")) (("Phil Pearl" NIL "phil" "perkpartners.com")) ((NIL NIL "phil" "perkpartners.com")) NIL NIL NIL "<25030.1273211740@local>") (("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 20 1 NIL NIL NIL NIL)("message" "rfc822" NIL NIL NIL "7bit" 1810 ("Fri, 07 May 2010 01:55:07 -0400" "wrap inner a" (("Phil Pearl" NIL "phil" "perkpartners.com")) (("Phil Pearl" NIL "phil" "perkpartners.com")) (("Phil Pearl" NIL "phil" "perkpartners.com")) ((NIL NIL "phil" "perkpartners.com")) NIL NIL NIL "<25015.1273211707@local>") (("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 27 3 NIL NIL NIL NIL)("message" "rfc822" NIL NIL NIL "7bit" 783 ("Fri, 07 May 2010 01:54:14 -0400" "inner msg #1" (("Phil Pearl" NIL "phil" "perkpartners.com")) (("Phil Pearl" NIL "phil" "perkpartners.com")) (("Phil Pearl" NIL "phil" "perkpartners.com")) ((NIL NIL "phil" "perkpartners.com")) NIL NIL NIL "<24986.1273211654@local>") ("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 25 3 NIL NIL NIL NIL) 23 NIL ("inline" ("filename" "52")) NIL NIL) "mixed" ("boundary" "=-=-=") NIL NIL NIL) 58 NIL ("inline" ("filename" "53")) NIL NIL) "mixed" ("boundary" "==-=-=") NIL NIL NIL) 91 NIL ("inline" ("filename" "52")) NIL NIL)("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 30 2 NIL NIL NIL NIL)("application" "octet-stream" NIL NIL "My attachment" "7bit" 76 NIL ("attachment" ("filename" ".signature.cell")) NIL NIL)("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 31 2 NIL NIL NIL NIL) "mixed" ("boundary" "===-=-=") NIL NIL NIL))};

$bsobj = Mail::IMAPClient::BodyStructure->new($bs8);
@exp   = qw(1 2 2.HEAD 2.TEXT 2.1 2.2 2.2.HEAD 2.2.TEXT 2.2.1 2.2.2 2.2.2.HEAD 2.2.2.TEXT 3 4 5);
ok( defined $bsobj, 'parsed eighth' );
is_deeply( [ $bsobj->parts ], \@exp, 'bs8 parts' )
  or diag( join(" ", $bsobj->parts ) );
