#!/usr/bin/perl
#
# tests for has_capability() and capability()
#
# capability() calls _imap_command('CAPABILITY') internally. rather
# than refactor just for testing, we subclass M::IC and use the
# overidden _imap_command() to feed it test data.

use strict;
use warnings;
use IO::Socket qw(:crlf);
use Test::More tests => 53;

BEGIN { use_ok('Mail::IMAPClient') or exit; }

# $comment, $capa, $exp
my @tests = (
    [
        "Example1 capability",
        [
            q{1 CAPABILITY},
q{* CAPABILITY IMAP4rev1 LITERAL+ SASL-IR ID ENABLE IDLE STARTTLS AUTH=PLAIN},
            q{1 OK Capability completed (0.001 + 0.000 secs).}
        ],
        [qw{IMAP4rev1 LITERAL+ SASL-IR ID ENABLE IDLE STARTTLS AUTH=PLAIN}],
    ],

    [
        "Example2 capability",
        [
q{* CAPABILITY IMAP4rev1 LITERAL+ SASL-IR LOGIN-REFERRALS ID ENABLE IDLE SORT SORT=DISPLAY THREAD=REFERENCES THREAD=REFS THREAD=ORDEREDSUBJECT MULTIAPPEND URL-PARTIAL CATENATE UNSELECT CHILDREN NAMESPACE UIDPLUS LIST-EXTENDED I18NLEVEL=1 CONDSTORE QRESYNC ESEARCH ESORT SEARCHRES WITHIN CONTEXT=SEARCH LIST-STATUS BINARY MOVE},
        ],
        [
            qw{IMAP4rev1 LITERAL+ SASL-IR LOGIN-REFERRALS ID ENABLE IDLE SORT SORT=DISPLAY THREAD=REFERENCES THREAD=REFS THREAD=ORDEREDSUBJECT MULTIAPPEND URL-PARTIAL CATENATE UNSELECT CHILDREN NAMESPACE UIDPLUS LIST-EXTENDED I18NLEVEL=1 CONDSTORE QRESYNC ESEARCH ESORT SEARCHRES WITHIN CONTEXT=SEARCH LIST-STATUS BINARY MOVE}
        ],
    ],
);

package Test::Mail::IMAPClient;

use base qw(Mail::IMAPClient);

sub new {
    my ( $class, %args ) = @_;
    my %me = %args;
    return bless \%me, $class;
}

# test stub
sub _imap_command {
    die("capability.t: unsupported command '$_[1]' for test\n")
      unless $_[1] eq "CAPABILITY";
}

# test stub
sub History {
    my ( $self, @args ) = @_;
    return @{ $self->{_next_history} || [] };
}

package main;

sub run_tests {
    my ($tests) = @_;

    my $imap = Test::Mail::IMAPClient->new( Uid => 0, Debug => 0 );

    # missing capability sanity checks
    is( $imap->has_capability("BOGUS"),
        undef, "scalar: no has_capability(BOGUS)=undef" );
    is_deeply( [ $imap->has_capability("BOGUS") ],
        [undef], "list: no has_capability(BOGUS)=[undef]" );

    for my $test (@$tests) {

        $imap = Test::Mail::IMAPClient->new( Uid => 0, Debug => 0 );
        my ( $comment, $capa, $exp ) = @$test;
        $imap->{_next_history} = $capa;

        my @r = $imap->capability();
        is_deeply( [ @r[ 0 .. $#{$exp} ] ], $exp, $comment . ": @$exp" );

        my ( $i, %cap );
        $cap{$_}++ for (@$exp);

        foreach my $e (@$exp) {
            ok( defined $imap->has_capability($e), "has_capability($e)" );
            my @kv = split( /=/, $e, 2 );
            if ( @kv == 2 ) {
                my $d =
                    ( $imap->has_capability( $kv[0] ) && $cap{ $kv[0] } )
                  ? [ 1, 1 ]
                  : [ undef, "undef" ];
                is( $imap->has_capability( $kv[0] ),
                    $d->[0],
                    "has_capability($kv[0]) (multival scalar) is $d->[1]" );
            }
            elsif ( !$i++ ) {
                is( $imap->has_capability($e), 1, "has_capability($e) eq '1'" );
            }
        }
    }
}

run_tests( \@tests );
