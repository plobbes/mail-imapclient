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
use Test::More tests => 70;

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
q{* CAPABILITY IMAP4rev1 LOGIN-REFERRALS SORT SORT=DISPLAY THREAD=REFERENCES THREAD=REFS THREAD=ORDEREDSUBJECT NAMESPACE UIDPLUS LIST-EXTENDED CONTEXT=SEARCH LIST-STATUS},
        ],
        [
            qw{IMAP4rev1 LOGIN-REFERRALS SORT SORT=DISPLAY THREAD=REFERENCES THREAD=REFS THREAD=ORDEREDSUBJECT NAMESPACE UIDPLUS LIST-EXTENDED CONTEXT=SEARCH LIST-STATUS}
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
        "", "scalar: no has_capability(BOGUS)=''" );    # was undef
    is_deeply( [ $imap->has_capability("BOGUS") ],
        [], "list: no has_capability(BOGUS)=[]" );      # was [undef]

    {
        my $cap =
q{* CAPABILITY IMAP4rev1 SORT SORT=DISPLAY I18NLEVEL=1 AUTH=PLAIN AUTH=XTEST};
        my $imap = Test::Mail::IMAPClient->new( Uid => 0, Debug => 0 );
        $imap->{_next_history} = [$cap];
        my ( $e, @e, $v, @v, @r );

        @e = (
            qw(IMAP4rev1 SORT SORT=DISPLAY I18NLEVEL=1 AUTH=PLAIN AUTH=XTEST I18NLEVEL AUTH)
        );
        @r = $imap->capability();
        is_deeply( \@r, \@e, "exp(@e)" );

	# imap4rev1 - case insensitive
	ok( $imap->has_capability("iMaP4ReV1"), 'has_capability("iMaP4ReV1")' );
	ok( $imap->imap4rev1(), 'imap4rev1()' );

        ( $e, @e ) = ( "SORT", qw(DISPLAY) );
        $v = $imap->has_capability($e);
        @v = $imap->has_capability($e);
        ok( $v,        "has_capability($e) true($v) - scalar [@e]" );
        ok( scalar @v, "has_capability($e) true(@v) - list (@e)" );

        ( $e, @e ) = ( "SORT=DISPLAY", qw(SORT=DISPLAY) );
        $v = $imap->has_capability($e);
        @v = $imap->has_capability($e);
        ok( $v,        "has_capability($e) true($v) - scalar [@e]" );
        ok( scalar @v, "has_capability($e) true(@v) - list (@e)" );

        ( $e, @e ) = ( "AUTH", qw(PLAIN XTEST) );
        $v = $imap->has_capability($e);
        @v = $imap->has_capability($e);
        ok( $v,        "has_capability($e) true($v) - scalar [@e]" );
        ok( scalar @v, "has_capability($e) true(@v) - list (@e)" );

        is_deeply( \@v, [@e], "mval($e) expect: [@e]" );
        $v = $imap->has_capability("AUTH=XTEST");
        is_deeply( $v, ["AUTH=XTEST"],
            "mval(AUTH=XTEST) expect: ['AUTH=XTEST']" );

        $v = $imap->has_capability("AUTH=NADA");
        @v = $imap->has_capability("AUTH=NADA");
        is_deeply( $v, "", "mval(AUTH=NADA) scalar expect: ''" );
        is_deeply( \@v, [], "mval(AUTH=NADA) list expect: ()" );
    }

    for my $test (@$tests) {

        $imap = Test::Mail::IMAPClient->new( Uid => 0, Debug => 0 );
        my ( $comment, $capa, $exp ) = @$test;
        $imap->{_next_history} = $capa;

        my @r = $imap->capability();
        is_deeply( [ @r[ 0 .. $#{$exp} ] ], $exp, $comment . ": @$exp" );

        foreach my $e (@$exp) {
            my $v = $imap->has_capability($e);
            my @v = $imap->has_capability($e);
            ok( $v,        "has_capability($e) true($v) - scalar" );
            ok( scalar @v, "has_capability($e) true(@v) - list" );

            my ( $k, $j ) = split( /=/, $e, 2 );
            if ( defined $j ) {
                my $v = $imap->has_capability($k);
                my @v = $imap->has_capability($k);
                ok( $v, "has_capability($k) true($v) - scalar mval [$e]" );
                ok( scalar @v, "has_capability($k) true(@v) - list mval ($e)" );
            }
        }
    }
}

run_tests( \@tests );
