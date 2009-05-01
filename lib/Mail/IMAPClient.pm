
# _{name} methods are undocumented and meant to be private.

use warnings;
use strict;

package Mail::IMAPClient;
our $VERSION = '3.17_02';

use Mail::IMAPClient::MessageSet;

use IO::Socket qw(:crlf SOL_SOCKET SO_KEEPALIVE);
use IO::Select();
use IO::File();
use Carp qw(carp); # $SIG{__WARN__} = \&Carp::cluck; #DEBUG

use Fcntl       qw(F_GETFL F_SETFL O_NONBLOCK);
use Errno       qw(EAGAIN EPIPE ECONNRESET);
use List::Util  qw(first min max sum);
use MIME::Base64 qw(encode_base64 decode_base64);
use File::Spec  ();

use constant APPEND_BUFFER_SIZE => 1024 * 1024;

use constant
  { Unconnected   => 0
  , Connected     => 1 # connected; not logged in
  , Authenticated => 2 # logged in; no mailbox selected
  , Selected      => 3 # mailbox selected
  };

use constant
  { INDEX         => 0 # Array index for output line number
  , TYPE          => 1 # Array index for line type (OUTPUT, INPUT, or LITERAL)
  , DATA          => 2 # Array index for output line data
  };

use constant NonFolderArg => 1;  # Value to pass to Massage to
                                 #    indicate non-folder argument

my %SEARCH_KEYS = map { ( $_ => 1 ) } qw/
    ALL ANSWERED BCC BEFORE BODY CC DELETED DRAFT FLAGGED
    FROM HEADER KEYWORD LARGER NEW NOT OLD ON OR RECENT
    SEEN SENTBEFORE SENTON SENTSINCE SINCE SMALLER SUBJECT
    TEXT TO UID UNANSWERED UNDELETED UNDRAFT UNFLAGGED
    UNKEYWORD UNSEEN/;

sub _debug
{   my $self = shift;
    return unless $self->Debug;

    my $text = join '', @_;
    $text    =~ s/$CRLF/\n  /og;
    $text    =~ s/\s*$/\n/;
    #use POSIX (); $text = POSIX::strftime("%F %T ", localtime).$text; #DEBUG
    my $fh   = $self->{Debug_fh} || \*STDERR;
    print $fh $text;
}

BEGIN {
   # set-up accessors
   foreach my $datum (
     qw(State Port Server Folder Peek User Password Timeout Buffer Debug
        Count Uid Debug_fh Maxtemperrors Authuser Authmechanism Authcallback
        Ranges Readmethod Showcredentials Prewritemethod Ignoresizeerrors
        Supportedflags Proxy Domain Maxcommandlength Keepalive))
   { no strict 'refs';
     *$datum = sub { @_ > 1 ? ($_[0]->{$datum} = $_[1]) : $_[0]->{$datum}
     };
   }
}

sub LastError
{   my $self = shift;
    @_ or return $self->{LastError};
    my $err = shift;
    $err =~ s/$CRLF$//o;
    # NOTE: older versions of Carp could cause $! to be reset
    $self->_debug( Carp::longmess("ERROR: $err") );
    $@ = $self->{LastError} = $err;
}

sub Fast_io(;$)
{   my ($self, $use) = @_;
    defined $use
       or return $self->{File_io};

    my $socket = $self->{Socket}
       or return;

    unless($use)
    {   eval { fcntl($socket, F_SETFL, delete $self->{_fcntl}) }
            if exists $self->{_fcntl};
        $@ = '';
        $self->{Fast_io} = 0;
        return;
    }

    my $fcntl = eval { fcntl($socket, F_GETFL, 0) };
    if($@)
    {   $self->{Fast_io} = 0;
        $self->_debug("not using Fast_IO; not available on this platform")
           unless $self->{_fastio_warning_}++;
        $@ = '';
        return;
    }

    $self->{Fast_io} = 1;
    my $newflags = $self->{_fcntl} = $fcntl;
    $newflags   |= O_NONBLOCK;
    fcntl($socket, F_SETFL, $newflags);
}

# removed
sub EnableServerResponseInLiteral {undef}

sub Wrap { shift->Clear(@_) }

# The following class method is for creating valid dates in appended msgs:

my @dow  = qw/Sun Mon Tue Wed Thu Fri Sat/;
my @mnt  = qw/Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec/;

sub Rfc822_date
{   my $class = shift;   #Date: Fri, 09 Jul 1999 13:10:55 -0000#
    my $date  = $class =~ /^\d+$/ ? $class : shift;  # method or function?
    my @date  = gmtime $date;

    sprintf "%s, %02d %s %04d %02d:%02d:%02d -%04d"
      , $dow[$date[6]], $date[3], $mnt[$date[4]], $date[5]+1900
      , $date[2], $date[1], $date[0], $date[8];
}

# The following class method is for creating valid dates for use
# in IMAP search strings:

sub Rfc2060_date
{   my $class = shift; # 11-Jan-2000 
    my $stamp = $class =~ /^\d+$/ ? $class : shift; # method or function
    my @date  = gmtime $stamp;

    sprintf "%02d-%s-%04d", $date[3], $mnt[$date[4]], $date[5]+1900;
}

sub Rfc2060_datetime($;$)
{   my ($class, $stamp, $zone) = @_; # 11-Jan-2000 04:04:04 +0000
    $zone   ||= '+0000';
    my @date  = gmtime $stamp;

    sprintf "%02d-%s-%04d %02d:%02d:%02d %s", $date[3], $mnt[$date[4]]
      , $date[5]+1900, $date[2], $date[1], $date[0], $zone;
}

# Change CRLF into \n

sub Strip_cr
{   my $class = shift;
    if( !ref $_[0] && @_==1 )
    {   (my $string = $_[0]) =~ s/$CRLF/\n/og;
        return $string;
    }

    wantarray
    ?   map { s/$CRLF/\n/ogm; $_ } (ref $_[0] ? @{$_[0]} : @_)
    : [ map { s/$CRLF/\n/ogm; $_ } (ref $_[0] ? @{$_[0]} : @_) ];
}

# The following defines a special method to deal with the Clear parameter:

sub Clear
{   my ($self, $clear) = @_;
    defined $clear or return $self->{Clear};

    my $oldclear   = $self->{Clear};
    $self->{Clear} = $clear;

    my @keys = reverse $self->_trans_index;

    for(my $i = $clear; $i < @keys ; $i++ )
    {   delete $self->{History}{$keys[$i]};
    }

    $oldclear;
}

# read-only access to the transaction number
sub Transaction { shift->Count };

# remove doubles from list
sub _remove_doubles(@) { my %seen; grep { ! $seen{$_}++ } @_ }

# the constructor:
sub new
{   my $class = shift;
    my $self  =
      { LastError     => "",
      , Uid           => 1
      , Count         => 0
      , Fast_io       => 1
      , Clear         => 5
      , Keepalive     => 0
      , Maxcommandlength => 1000,
      , Maxtemperrors => 'unlimited'
      , State         => Unconnected
      , Authmechanism => 'LOGIN'
      , Port          => 143
      , Timeout       => 600
      , History       => {}
      };
    while(@_)
    {   my $k = ucfirst lc shift;
        my $v = shift;
        $self->{$k} = $v if defined $v;
    }
    bless $self, ref($class)||$class;

    if(my $sup = $self->{Supportedflags})  # unpack into case-less HASH
    {   my %sup = map { m/^\\?(\S+)/ ? lc $1 : () } @$sup;
        $self->{Supportedflags} = \%sup;
    }

    $self->{Debug_fh} ||= \*STDERR;
    CORE::select((select($self->{Debug_fh}),$|++)[0]);

    if($self->Debug)
    {   $self->_debug("Started at ".localtime());
        $self->_debug("Using Mail::IMAPClient version $VERSION on perl $]");
    }

    $self->Socket($self->{Socket})
        if $self->{Socket};

    if($self->{Rawsocket})
    {
        my $sock = delete $self->{Rawsocket};
        # Ignore Rawsocket if Socket has already been set -- TODO carp/croak?
        $self->RawSocket($sock) unless $self->{Socket};
    }

    !$self->{Socket} && $self->{Server} ? $self->connect : $self;
}

sub connect(@)
{   my $self = shift;
    %$self = (%$self, @_);

    my $server  = $self->Server;
    my $port    = $self->Port;
    my @timeout = $self->Timeout ? (Timeout => $self->Timeout) : ();

    my $sock;

    if(File::Spec->file_name_is_absolute($server))
    {   $self->_debug("Connecting to unix socket $server");
        $sock = IO::Socket::UNIX->new
         ( Peer  => $server
         , Debug => $self->Debug
         , @timeout
         );
    }
    else
    {   $self->_debug("Connecting to $server port $port");
        $sock = IO::Socket::INET->new
          ( PeerAddr => $server
          , PeerPort => $port
          , Proto    => 'tcp'
          , Debug    => $self->Debug
          , @timeout
          );
    }

    unless($sock)
    {   $self->LastError("Unable to connect to $server: $@");
        return undef;
    }

    $self->_debug("Connected to $server");
    $self->Socket($sock);
}

sub RawSocket(;$)
{   my ($self, $sock) = @_;
    defined $sock
        or return $self->{Socket};

    $self->{Socket} = $sock;
    $self->{_select} = IO::Select->new($sock);

    delete $self->{_fcntl};
    $self->Fast_io($self->Fast_io);
 
    $sock;
}

sub Socket($)
{   my ($self, $sock) = @_;
    defined $sock
        or return $self->{Socket};

    $self->RawSocket($sock);
    $self->State(Connected);

    setsockopt($sock, SOL_SOCKET, SO_KEEPALIVE, 1) if $self->Keepalive;

    my $code;
  LINE:
    while(my $output = $self->_read_line)
    {   foreach my $o (@$output)
        {   $self->_record($self->Count, $o);
            next unless $o->[TYPE] eq "OUTPUT";

            $code = $o->[DATA] =~ /^\*\s+(OK|BAD|NO|PREAUTH)/i ? uc($1) : undef;
            last LINE;
        }
    }
    $code or return undef;

    if($code eq 'BYE' || $code eq 'NO')
    {   $self->State(Unconnected);
        return undef;
    }

    if($code eq 'PREAUTH')
    {   $self->State(Authenticated);
        return $self;
    }

    $self->User && $self->Password ? $self->login : $self;
}

sub login
{   my $self = shift;
    my $auth = $self->Authmechanism;
    return $self->authenticate($auth, $self->Authcallback)
        if $auth && $auth ne 'LOGIN';

    my $passwd = $self->Password;
    if($passwd =~ m/\W/)  # need to quote
    {   $passwd =~ s/(["\\])/\\$1/g;
        $passwd = qq{"$passwd"};
    }

    my $id     = $self->User;
    $id        = qq{"$id"} if $id !~ /^".*"$/;

    $self->_imap_command("LOGIN $id $passwd")
        or return undef;

    $self->State(Authenticated);
    $self;
}

sub noop
{   my ($self, $user) = @_;
    $self->_imap_command("NOOP") ? $self->Results : undef;
}

sub proxyauth
{   my ($self, $user) = @_;
    $self->_imap_command("PROXYAUTH $user") ? $self->Results : undef;
}

sub separator
{   my ($self, $target) = @_;
    unless(defined $target)
    {   # separator is namespace's 1st thing's 1st thing's 2nd thing:
        my $ns = $self->namespace;
        return undef if $self->LastError;
        if ($ns) {
            my $sep = $ns->[0][0][1];
            return $sep if $sep;
        }
        $target = '';
    }

    return $self->{separators}{$target}
        if exists $self->{separators}{$target};

    my $list = $self->list(undef, $target) || [];
    return undef if $self->LastError;

    foreach my $line (@$list)
    {   my $rec = $self->_list_response_parse($line);
        next unless defined $rec->{name};
        $self->{separators}{ $rec->{name} } = $rec->{delim};
    }
    return $self->{separators}{$target};
}

sub sort
{   my ($self, $crit, @a) = @_;

    $crit =~ /^\(.*\)$/        # wrap criteria in parens
        or $crit = "($crit)";

    $self->_imap_uid_command(SORT => $crit, @a)
         or return wantarray ? () : [];

    my @results = $self->History;
    my @hits;
    foreach (@results)
    {   chomp;
        s/\r$//;
        s/^\*\s+SORT\s+// or next;
        push @hits, grep /\d/, split;
    }
    wantarray ? @hits : \@hits;
}

sub list
{   my ($self, $reference, $target) = @_;
    defined $reference or $reference = "";
    defined $target    or $target    = '*';
    length $target     or $target    = '""';

    $target eq '*' || $target eq '""'
         or $target = $self->Massage($target);

    $self->_imap_command( qq[LIST "$reference" $target] )
        or return undef;

    wantarray ? $self->History : $self->Results;
}

sub lsub
{   my ($self, $reference, $target) = @_;
    defined $reference or $reference = "";
    defined $target    or $target = '*';
    $target = $self->Massage($target);

    $self->_imap_command( qq[LSUB "$reference" $target] )
         or return undef;

    wantarray ? $self->History : $self->Results;
}

sub subscribed
{   my ($self, $what) = @_;
    my $known = $what ? $what.$self->separator($what)."*" : undef;

    my @list = $self->lsub(undef, $known);
    push @list, $self->lsub(undef, $what) if $what && $self->exists($what);

    my @folders;
    for(my $m = 0; $m < @list; $m++ )
    {   $list[$m] or next;

        if($list[$m] !~ /$CRLF$/o)
        {   $list[$m]  .= $list[$m+1];
            $list[$m+1] = "";
        }

        # $self->_debug("Subscribed: examining $list[$m]");

        push @folders, $1||$2
            if $list[$m] =~
                m/ ^ \* \s+ LSUB            # * LSUB
                     \s+ \( [^\)]* \) \s+   # (Flags)
                     (?:"[^"]*"|NIL)\s+     # "delimiter" or NIL
                     (?:"([^"]*)"|(.*))$CRLF$  # Name or "Folder name"
                 /oix;
    }

    my @clean = _remove_doubles @folders;
    wantarray ? @clean : \@clean;
}

sub deleteacl
{   my ($self, $target, $user) = @_;
    $target = $self->Massage($target);
    $user   =~ s/^"(.*)"$/$1/;
    $user   =~ s/"/\\"/g;

    $self->_imap_command( qq[DELETEACL $target "$user"] )
        or return undef;

    wantarray ? $self->History : $self->Results;
}

sub setacl
{   my ($self, $target, $user, $acl) = @_;
    $target ||= $self->Folder;
    $target = $self->Massage($target);

    $user ||= $self->User;
    $user   =~ s/^"(.*)"$/$1/;
    $user   =~ s/"/\\"/g;

    $acl    =~ s/^"(.*)"$/$1/;
    $acl    =~ s/"/\\"/g;

    $self->_imap_command( qq[SETACL $target "$user" "$acl"] )
        or return undef;

    wantarray ? $self->History : $self->Results;
}


sub getacl
{   my ($self, $target) = @_;
    defined $target or $target = $self->Folder;
    my $mtarget = $self->Massage($target);
    $self->_imap_command( qq[GETACL $mtarget] )
        or return undef;

    my @history = $self->History;
    my $hash;
    for(my $x = 0; $x < @history; $x++ )
    {
        next if $history[$x] !~ /^\* ACL/;

        my $perm = $history[$x]=~ /^\* ACL $/
                 ? $history[++$x].$history[++$x]
                 : $history[$x];

        $perm =~ s/\s?$CRLF$//o;
        until( $perm =~ /\Q$target\E"?$/ || !$perm)
        {   $perm =~ s/\s([^\s]+)\s?$// or last;
            my $p = $1;
            $perm =~ s/\s([^\s]+)\s?$// or last;
            my $u = $1;
            $hash->{$u} = $p;
            $self->_debug("Permissions: $u => $p");
        }
    }
    $hash;
}

sub listrights
{   my ($self, $target, $user) = @_;
    $target ||= $self->Folder;
    $target   = $self->Massage($target);

    $user   ||= $self->User;
    $user     =~ s/^"(.*)"$/$1/;
    $user     =~ s/"/\\"/g;

    $self->_imap_command( qq[LISTRIGHTS $target "$user"] )
        or return undef;

    my $resp   = first { /^\* LISTRIGHTS/ } $self->History;
    my @rights = split /\s/, $resp;
    my $rights = join '', @rights[4..$#rights];
    $rights    =~ s/"//g;
    wantarray ? split(//, $rights) : $rights;
}

sub select
{   my ($self, $target) = @_;
    defined $target or return undef;

    my $qqtarget = $self->Massage($target);
    my $old = $self->Folder;

    $self->_imap_command("SELECT $qqtarget")
        or return undef;

    $self->State(Selected);
    $self->Folder($target);
    $old || $self;  # ??$self??
}

sub message_string
{   my ($self, $msg) = @_;

    my $peek = $self->Peek ? '.PEEK' : '';
    my $cmd  = $self->imap4rev1 ? "BODY$peek\[]" : "RFC822$peek";

    $self->fetch($msg, $cmd)
        or return undef;

    my $string = $self->_transaction_literals;

    unless($self->Ignoresizeerrors)
    {   # Check size with expected size
        my $expected_size = $self->size($msg);
        unless(defined $expected_size)
        {   $self->LastError( "message_string() cannot get message size,"
                            . " you may need the IgnoreSizeErrors option");
            return undef;
        }

        # RFC822.SIZE may be wrong.
        # See RFC2683 3.4.5 "RFC822.SIZE"
        if(length($string) != $expected_size)
        {   $self->LastError("message_string() "
              . "expected $expected_size bytes but received ".length($string));
            return undef;
        }
    }

    $string;
}

sub bodypart_string
{   my($self, $msg, $partno, $bytes, $offset) = @_;

    unless($self->imap4rev1)
    {   $self->LastError("Unable to get body part; server ".$self->Server
                . " does not support IMAP4REV1");
        return undef;
    }

    $offset ||= 0;
    my $cmd = "BODY" . ($self->Peek ? '.PEEK' : '') . "[$partno]"
            . ($bytes ? "<$offset.$bytes>" : '');

    $self->fetch($msg, $cmd)
        or return undef;

    $self->_transaction_literals;
}

sub message_to_file
{   my $self = shift;
    my $fh   = shift;
    my $msgs = join ',', @_;

    my $handle;
    if(ref $fh) { $handle = $fh }
    else
    {   $handle = IO::File->new(">>$fh");
        unless(defined($handle))
        {   $self->LastError("Unable to open $fh: $!");
            return undef;
        }
        binmode $handle; # For those of you who need something like this...
    }

    my $clear = $self->Clear;
    $self->Clear($clear)
        if $self->Count >= $clear && $clear > 0;

    my $peek   = $self->Peek ? '.PEEK' : '';
    my $cmd    = $self->imap4rev1 ? "BODY$peek\[]" : "RFC822$peek";
    my $uid    = $self->Uid ? "UID " : "";
    my $trans  = $self->Count($self->Count+1);
    my $string = "$trans ${uid}FETCH $msgs $cmd";

    $self->_record($trans, [0, "INPUT", $string] );

    my $feedback = $self->_send_line($string);
    unless($feedback)
    {    $self->LastError("Error sending '$string': " . $self->LastError);
         return undef;
    }

    my $code;

  READ:
    until($code)
    {   my $output = $self->_read_line($handle)
            or return undef;

        foreach my $o (@$output)
        {   $self->_record($trans,$o);
            next unless $self->_is_output($o);

            $code = $o->[DATA] =~ /^$trans\s+(OK|BAD|NO)/mi ? $1 : undef;
            if($o->[DATA] =~ /^\*\s+BYE/im)
            {    $self->State(Unconnected);
                 return undef;
            }
        }
    }
    ref $fh or close $handle;
    $code =~ /^OK/im ? $self : undef;
}

sub message_uid
{   my ($self, $msg) = @_;

    foreach ($self->fetch($msg, "UID"))
    {   return $1 if m/\(UID\s+(\d+)\s*\)\r?$/;
    }
    undef;
}

#???? this code is very clumpsy, and currently probably broken.
#  Why not use a pipe???
#  Is a quadratic slowdown not much simpler and better???
#  Shouldn't the slowdowns extend over multiple messages?
#  --> create clean read and write methods

sub migrate
{   my ($self, $peer, $msgs, $folder) = @_;
    my $toSock     = $peer->Socket,
    my $fromSock   = $self->Socket;
    my $bufferSize = $self->Buffer || 4096;

    local $SIG{PIPE} = 'IGNORE';  # avoid SIGPIPE on syswrite, handle as error

    unless(eval {$peer->IsConnected} )
    {    $self->LastError("Invalid or unconnected " .  ref($self)
             . " object used as target for migrate. $@");
        return undef;
    }

    unless($folder)
    {   unless($folder = $self->Folder)
        {   $self->LastError( "No folder selected on source mailbox.");
            return undef;
        }

        unless($peer->exists($folder) || $peer->create($folder))
        {   $self->LastError("Unable to create folder $folder on target "
                 . "mailbox: ". $peer->LastError);
            return undef
        };
    }

    defined $msgs or $msgs = "ALL";
    $msgs = $self->search("ALL")
        if uc $msgs eq 'ALL';

    my $range = $self->Range($msgs);
    my $clear = $self->Clear;

    $self->_debug("Migrating the following msgs from $folder: $range");
  MSG:
    foreach my $mid ($range->unfold)
    {
        $self->_debug("Migrating message $mid in folder $folder");

        my $leftSoFar = my $size = $self->size($mid);

        # fetch internaldate and flags of original message:
        my $intDate = $self->internaldate($mid);
        my @flags   = grep !/\\Recent/i, $self->flags($mid);
        my $flags   = join ' ', $peer->supported_flags(@flags);

        # set up transaction numbers for from and to connections:
        my $trans   = $self->Count($self->Count+1);
        my $ptrans  = $peer->Count($peer->Count+1);

        # If msg size is less than buffersize then do whole msg in one
        # transaction:
        if($size <= $bufferSize)
        {   my $new_mid = $peer->append_string
               ($folder, $self->message_string($mid), $flags, $intDate);

            unless(defined $new_mid)
            {   $self->LastError("Unable to append to $folder "
                   . "on target mailbox. ".  $peer->LastError);
                return undef;
            }

            $self->_debug("Copied message $mid in folder $folder to "
                . $peer->User . '@' . $peer->Server
                . ". New message UID is $new_mid")
                if $self->Debug;

            $peer->_debug("Copied message $mid in folder $folder from "
                .  $self->User .  '@' . $self->Server
                . ". New message UID is $new_mid")
                if $peer->Debug;

            next MSG;
        }

        # otherwise break it up into digestible pieces:
        my ($cmd, $extract_size);
        if($self->imap4rev1)
        {   $cmd = $self->Peek ? 'BODY.PEEK[]' : 'BODY[]';
            $extract_size = sub { $_[0] =~ /\(.*BODY\[\]<\d+> \{(\d+)\}/i; $1 };
        }
        else
        {   $cmd = $self->Peek ? 'RFC822.PEEK' : 'RFC822';
            $extract_size = sub { $_[0] =~ /\(RFC822\[\]<\d+> \{(\d+)\}/i; $1 };
        }

        # Now let's warn the peer that there's a message coming:

        my $pstring = "$ptrans APPEND " . $self->Massage($folder)
           . (length $flags ? " ($flags)" : '') . qq[ "$intDate" {$size}];

        $self->_debug("About to issue APPEND command to peer for msg $mid");

        $peer->_record($ptrans, [0, "INPUT", $pstring] );
        unless($peer->_send_line($pstring))
        {   $self->LastError("Error sending '$pstring': " . $self->LastError);
            return undef;
        }

        # Get the "+ Go ahead" response:
        my $code;
        until(defined $code)
        {
             my $readSoFar  = 0;
             my $fromBuffer = '';
             $readSoFar += sysread($toSock, $fromBuffer, 1, $readSoFar) || 0
                 until $fromBuffer =~ /$CRLF/o;

             $code = $fromBuffer =~ /^\+/ ? 'OK'
                   : $fromBuffer =~ /^(?:\d+\s(BAD|NO|OK))/ ? $1 : undef;

             $peer->_debug("$folder: received $fromBuffer from server");

             if($fromBuffer =~ /^\*\s+BYE/i)
             {   $self->State(Unconnected);
                 return undef;
             }

             # ... and log it in the history buffers
             $self->_record($trans, [0, "OUTPUT",
     "Mail::IMAPClient migrating message $mid to $peer->User\@$peer->Server"] );
             $peer->_record($ptrans, [0, "OUTPUT", $fromBuffer] );
        }

        if($code ne 'OK')
        {   $self->_debug("Error writing to target host: $@");
            next MIGMSG;
        }

        # Here is where we start sticking in UID if that parameter
        # is turned on:
        my $string = ($self->Uid ? "UID " : "") . "FETCH $mid $cmd";

        # Clean up history buffer if necessary:
        $self->Clear($clear)
            if $self->Count >= $clear && $clear > 0;

       # position will tell us how far from beginning of msg the
       # next IMAP FETCH should start (1st time start at offet zero):
       my $position   = 0;
       my $chunkCount = 0;
       my $readSoFar  = 0;
       while($leftSoFar > 0)
       {   my $take      = min $leftSoFar, $bufferSize;
           my $newstring = "$trans $string<$position.$take>";

            $self->_record($trans, [0, "INPUT", $newstring] );
            $self->_debug("Issuing migration command: $newstring");

            unless($self->_send_line($newstring))
            {   $self->LastError("Error sending '$newstring' to source IMAP: "
                  . $self->LastError);
                return undef;
            }

            my $chunk;
            my $fromBuffer = "";
            until($chunk = $extract_size->($fromBuffer))
            {   $fromBuffer = '';
                sysread($fromSock, $fromBuffer, 1, length $fromBuffer)
                    until $fromBuffer =~ /$CRLF$/o;

                $self->_record($trans, [0, "OUTPUT", $fromBuffer]);

                if($fromBuffer =~ /^$trans (?:NO|BAD)/ )
                {   $self->LastError($fromBuffer);
                    next MIGMSG;
                }

                if($fromBuffer =~ /^$trans (?:OK)/ )
                {   $self->LastError("Unexpected good return code " .
                        "from source host: $fromBuffer");
                    next MIGMSG;
                }
            }

            $fromBuffer = "";
            while($readSoFar < $chunk)
            {   $readSoFar += sysread($fromSock, $fromBuffer
                                 , $chunk-$readSoFar,$readSoFar) ||0;
            }

            my $wroteSoFar = 0;
            my $temperrs   = 0;
            my $waittime   = .02;
            my $maxwrite   = 0;
            my $maxagain   = $self->Maxtemperrors || 10;
            undef $maxagain if $maxagain eq 'unlimited';
            my @previous_writes;

            while($wroteSoFar < $chunk)
            {   while($wroteSoFar < $readSoFar)
                {   my $ret = syswrite($toSock, $fromBuffer
                       , $chunk - $wroteSoFar, $wroteSoFar);

                    if(defined $ret)
                    {   $wroteSoFar += $ret;
                        $maxwrite = max $maxwrite, $ret;
                        $temperrs = 0;
                    }

                    if($! == EPIPE or $! == ECONNRESET)
                    {   $self->LastError("Write failed '$!'");
                        $self->State(Unconnected);
                        return undef;
                    }

                    if($! == EAGAIN || $ret==0)
                    {   if(defined $maxagain && $temperrs++ > $maxagain)
                        {   $self->LastError("Persistent error '$!'");
                            return undef;
                        }

                        $waittime = $self->_optimal_sleep($maxwrite,
                             $waittime, \@previous_writes);
                        next;
                    }

                    $self->State(Unconnected) if($! == EPIPE or $! == ECONNRESET);
                    $self->LastError("Write failed '$!'");
                    return;  # no luck
                }

                $peer->_debug("Chunk $chunkCount: wrote $wroteSoFar (of $chunk)");
            }
        }

        $position  += $readSoFar;
        $leftSoFar -= $readSoFar;
        my $fromBuffer = "";

        # Finish up reading the server response from the fetch cmd
        #     on the source system:

        undef $code;
        until($code)
        {   $self->_debug("Reading from source server; expecting ') OK' type response");
            my $output = $self->_read_line or return undef;
            foreach my $o (@$output)
            {   $self->_record($trans, $o);
                $self->_is_output($o) or next;

                $code = $o->[DATA] =~ /^$trans (OK|BAD|NO)/mi ? $1 : undef;
                if($o->[DATA] =~ /^\*\s+BYE/im)
                {   $self->State(Unconnected);
                   return undef;
                }
            }
        }

        # Now let's send a <CR><LF> to the peer to signal end of APPEND cmd:
        {   my $wroteSoFar = 0;
            my $fromBuffer = $CRLF;
            until($wroteSoFar >= 2) {
                $wroteSoFar += syswrite($toSock,$fromBuffer,2-$wroteSoFar,$wroteSoFar)||0;
                if($! == EPIPE or $! == ECONNRESET)
                {   $self->LastError("Write failed '$!'");
                    $self->State(Unconnected);
                    return undef;
                }
            }
        }

        # Finally, let's get the new message's UID from the peer:
        my $new_mid;
        undef $code;
        until($code)
        {   $peer->_debug("Reading from target: expect new uid in response");

            my $output = $peer->_read_line or last;
            foreach my $o (@$output)
            {   $peer->_record($ptrans,$o);
                next unless $peer->_is_output($o);

                $code    = $o->[DATA] =~ /^$ptrans (OK|BAD|NO)/mi ? $1 : undef;
                $new_mid = $o->[DATA] =~ /APPENDUID \d+ (\d+)/ ? $1 : undef
                    if $code;

                if($o->[DATA] =~ /^\*\s+BYE/im)
                {   $peer->State(Unconnected);
                    return undef;
                }
            }

            $new_mid ||= "unknown";
        } 
        if($self->Debug)
        {   $self->_debug("Copied message $mid in folder $folder to "
          . $peer->User.'@'.$peer->Server. ". New Message UID is $new_mid");

            $peer->_debug("Copied message $mid in folder $folder from "
          . $self->User.'@'.$self->Server . ". New Message UID is $new_mid");
        }
    }

    $self;
}

# Optimization of wait time between syswrite calls only runs if syscalls
# run too fast and fill the buffer causing "EAGAIN: Resource Temp. Unavail"
# errors. The premise is that $maxwrite will be approx. the same as the
# smallest buffer between the sending and receiving side. Waiting time
# between syscalls should ideally be exactly as long as it takes the
# receiving side to empty that buffer, minus a little bit to prevent it
# from emptying completely and wasting time in the select call.

sub _optimal_sleep($$$)
{   my ($self, $maxwrite, $waittime, $last5writes) = @_;

    push  @$last5writes, $waittime;
    shift @$last5writes if @$last5writes > 5;

    my $bufferavail = (sum @$last5writes) / @$last5writes;

    if($bufferavail < .4 * $maxwrite)
    {   # Buffer is staying pretty full; we should increase the wait
        # period to reduce transmission overhead/number of packets sent
        $waittime *= 1.3;
    }
    elsif($bufferavail > .9 * $maxwrite)
    {   # Buffer is nearly or totally empty; we're wasting time in select
        # call that could be used to send data, so reduce the wait period
        $waittime *= .5;
    }

    CORE::select(undef, undef, undef, $waittime);
    $waittime;
}

sub body_string
{   my ($self, $msg) = @_;
    my $ref = $self->fetch($msg, "BODY" .($self->Peek ? ".PEEK" : "")."[TEXT]");

    my $string = join '', map {$_->[DATA]}
        grep {$self->_is_literal($_)} @$ref;

    return $string
        if $string;

    my $head;
    while($head = shift @$ref)
    {   $self->_debug("body_string: head = '$head'");

        last if $head =~
            /(?:.*FETCH .*\(.*BODY\[TEXT\])|(?:^\d+ BAD )|(?:^\d NO )/i;
    }

    unless(@$ref)
    {   $self->LastError("Unable to parse server response from ".$self->LastIMAPCommand);
        return undef;
    }

    my $popped;
    $popped = pop @$ref    # (-: vi
        until ($popped && $popped =~ /\)$CRLF$/o)  # (-: vi
           || ! grep /\)$CRLF$/o, @$ref;

     if($head =~ /BODY\[TEXT\]\s*$/i )
     {   # Next line is a literal
         $string .= shift @$ref while @$ref;
         $self->_debug("String is now $string")
             if $self->Debug;
     }

     $string;
}

sub examine
{   my ($self, $target) = @_;
    defined $target or return undef;

    $self->_imap_command('EXAMINE ' . $self->Massage($target))
        or return undef;

    my $old = $self->Folder;
    $self->Folder($target);
    $self->State(Selected);
    $old || $self;
}

sub idle
{   my $self  = shift;
    my $good  = '+';
    my $count = $self->Count +1;
    $self->_imap_command("IDLE", $good) ? $count : undef;
}

sub done
{   my $self  = shift;
    my $count = shift || $self->Count;

    my $clear = $self->Clear;
    $self->Clear($clear)
        if $self->Count >= $clear && $clear > 0;

    my $string = "DONE$CRLF";
    $self->_record($count, [$self->_next_index($count), "INPUT", $string] );

    unless($self->_send_line($string, 1))
    {   $self->LastError("Error sending '$string': " . $self->LastError);
        return undef;
    }

    my $code;
    until($code && $code =~ /OK|BAD|NO/m)
    {   my $output = $self->_read_line or return undef;
        for my $o (@$output)
        {   $self->_record($count,$o);
            next unless $self->_is_output($o);
            $code = $o->[DATA] =~ /^(?:$count) (OK|BAD|NO)/m ? $1 : undef;
            $self->State(Unconnected) if $o->[DATA] =~ /^\*\s+BYE/;
        }
    }

    $code eq 'OK' ? @{$self->Results} : undef;  #?? enforce list context?
}

sub tag_and_run
{   my ($self, $string, $good) = @_;
    $self->_imap_command($string, $good);
    @{$self->Results};  #??? enforce list context
}


# _imap_command runs a command, inserting the correct tag and <CR><LF> and whatnot.
# When updating _imap_command, remember to examine the run() method too, since
# it is very similar.

sub _imap_command
{   my $self   = shift;
    my $string = shift or return undef;
    my $good   = shift || 'GOOD';
    my $qgood  = quotemeta $good;

    my $clear = $self->Clear;
    $self->Clear($clear)
        if $self->Count >= $clear && $clear > 0;

    my $count  = $self->Count($self->Count+1);
    $string    = "$count $string";

    $self->_record($count, [0, "INPUT", $string] );

    unless($self->_send_line($string))
    {   $self->LastError("Error sending '$string': " . $self->LastError);
        return undef;
    }

    my ( $code, $data );

   READ:
    until($code)
    {   my $output = $self->_read_line or return undef;
        foreach my $o (@$output)
        {   $self->_record($count, $o);
            $self->_is_output($o) or next;

            if($good eq '+' && $o->[DATA] =~ /^$qgood/m)
            {   $code = $qgood;
            }
            elsif($o->[DATA] =~ /^$count\s+(OK|BAD|NO|$qgood)/mi)
            {   ($code) = $1;
                $data = $o->[DATA];
            }

            if($o->[DATA] =~ /^\*\s+BYE/im)
            {   $self->State(Unconnected);
                $self->LastError($o->[DATA]) unless $string eq "$count LOGOUT";
                return undef;
            }
        }
    }

    if ( $code eq "OK" or $code eq $good )
    {   return $self;
    }
    else
    {   $self->LastError($data);
        return undef;
    }
}

sub _imap_uid_command
{   my ($self, $cmd) = (shift, shift);
    my $args = @_ ? join(" ", '', @_) : '';
    my $uid  = $self->Uid ? 'UID ' : '';
    $self->_imap_command("$uid$cmd$args");
}

sub run
{   my $self   = shift;
    my $string = shift or return undef;
    my $good   = shift || 'GOOD';
    my $qgood  = quotemeta $good;

    my $count  = $self->Count($self->Count+1);
    my $tag    = $string =~ /^(\S+) / ? $1 : undef;

    $tag or $self->LastError("Invalid string passed to run method; no tag found.");

    my $clear  = $self->Clear;
    $self->Clear($clear)
        if $self->Count >= $clear && $clear > 0;

    $self->_record($count, [$self->_next_index($count), "INPUT", $string] );

    unless($self->_send_line($string, 1))
    {   $self->LastError("Error sending '$string': " . $self->LastError);
        return undef;
    }

    my $code = '';
    until($code =~ /(OK|BAD|NO|$qgood)/m )
    {   my $output = $self->_read_line or return undef;
        foreach my $o (@$output)
        {   $self->_record($count, $o);
            $self->_is_output($o) or next;

            if($good eq '+' && $o->[DATA] =~ /^$qgood/mi)
            {   $code = $qgood;
            }
            else
            {   ($code) = $o->[DATA] =~ /^(?:$tag|\*) (OK|BAD|NO|$qgood)/mi;
            }

            $self->State(Unconnected)
                if $o->[DATA] =~ /^\*\s+BYE/;
        }
    }

    $tag eq $count
        or $self->{History}{$tag} = $self->{History}{$count};

    $code =~ /^OK|$qgood/ ? @{$self->Results} : undef;
}

# _record saves the conversation into the History structure:
sub _record
{   my ($self, $count, $array) = @_;
    if ($array->[DATA] =~ /^\d+ LOGIN/i && !$self->Showcredentials)
    {   $array->[DATA] =~ s/LOGIN.*/LOGIN XXXXXXXX XXXXXXXX/i;
    }

    push @{$self->{History}{$count}}, $array;
}

#_send_line writes to the socket:
sub _send_line
{   my ($self, $string, $suppress) = (shift, shift, shift);

    $string =~ s/\r?\n?$/$CRLF/o
        unless $suppress;

    if($string =~ s/^([^\n{]*\{(\d+)\}$CRLF)(.)/$3/o)  # ;-} vi
    {   # Line starts with literal
        my ($first, $len) = ($1, $2);
        $self->_debug("Sending literal: $first\tthen: $string");

        $self->_send_line($first) or return undef;
        my $output = $self->_read_line or return undef;

        my $code;
        foreach my $o (@$output)
        {   $self->_record($self->Count, $o);
            if($o->[DATA] =~ /^\+/)
            {   $code = 1;
            }
            elsif($o->[DATA] =~ /^\*\s+BYE/)
            {   $self->State(Unconnected);
                $self->close;
                $self->LastError($o->[DATA]);
                return undef;
            }
            elsif($o->[DATA]=~ /^\S+\s+(?:NO|BAD)/i )
            {   $self->LastError($o->[DATA]);
                return undef;
            }
        }

        unless($code)
        {
            $self->LastError( "unexpected response: " . join(" ", @$output) )
              unless $self->LastError;
            return undef;
        }

        # the second part follows the non-literal output, as below.
    }

    unless($self->IsConnected)
    {   $self->LastError("NO not connected");
        return undef;
    }

    if(my $prew = $self->Prewritemethod)
    {   $string = $prew->($self, $string);
    }

    $self->_debug("Sending: $string");
    $self->_send_bytes(\$string);
}

sub _send_bytes($)
{   my ($self, $byteref) = @_;
    my ($total, $temperrs, $maxwrite) = (0, 0, 0);
    my $waittime = .02;
    my @previous_writes;

    my $maxagain = $self->Maxtemperrors || 10;
    undef $maxagain if $maxagain eq 'unlimited';

    local $SIG{PIPE} = 'IGNORE';   # handle SIGPIPE as normal error

    while($total < length $$byteref)
    {   my $written = syswrite($self->Socket, $$byteref, length($$byteref)-$total,
                    $total);

        if(defined $written)
        {   $temperrs = 0;
            $total   += $written;
            next;
        }

        if($! == EAGAIN)
        {   if(defined $maxagain && $temperrs++ > $maxagain)
            {   $self->LastError("Persistent error '$!'");
                return undef;
            }

            $waittime = $self->_optimal_sleep($maxwrite, $waittime, \@previous_writes);
            next;
        }

        # Unconnected might be apropos for more than just these?
        my $emsg = $! ? "$!" : "no error caught";
        $self->State(Unconnected) if($! == EPIPE or $! == ECONNRESET);
        $self->LastError("Write failed '$emsg'");

        return undef;  # no luck
    }

    $self->_debug("Sent $total bytes");
    $total;
}

# _read_line: read one line from the socket

# It is also re-implemented in: message_to_file
#
# $output = $self->_read_line($literal_callback, $output_callback)
#    Both input arguments are optional, but if supplied must either
#    be a filehandle, coderef, or undef.
#
#    Returned argument is a reference to an array of arrays, ie:
#    $output = [
#            [ $index, 'OUTPUT'|'LITERAL', $output_line ] ,
#            [ $index, 'OUTPUT'|'LITERAL', $output_line ] ,
#            ...     # etc,
#    ];

sub _read_line
{   my ($self, $literal_callback, $output_callback) = @_;

    my $socket = $self->Socket;
    unless($self->IsConnected && $socket)
    {   $self->LastError("NO not connected");
        return undef;
    }

    my $iBuffer  = "";
    my $oBuffer  = [];
    my $index    = $self->_next_index;
    my $timeout  = $self->Timeout;
    my $readlen  = $self->{Buffer} || 4096; 

    until(@$oBuffer # there's stuff in output buffer:
      && $oBuffer->[-1][TYPE] eq 'OUTPUT' # that thing is an output line:
      && $oBuffer->[-1][DATA] =~ /\r?\n$/ # the last thing there has cr-lf:
      && !length $iBuffer                 # and the input buffer has been MT'ed:
    )
    {   my $transno = $self->Transaction;

        if($timeout)
        {    my $rc = _read_more($socket, $timeout);
             unless($rc > 0)
             {   my $msg = ( $rc ? "error" : "timeout" )
                   . " waiting ${timeout}s for data from server"
                   . ( $! ? ": $!" : "" );
                 $self->LastError($msg);
                 $self->_record($transno,
                   [ $self->_next_index($transno), "ERROR"
                   , "$transno * NO $msg"]);
                 $self->_disconnect; # BUG: can not handle timeouts gracefully
                 return undef;
             }
        }

        my $emsg;
        my $ret = $self->_sysread($socket, \$iBuffer, $readlen,length $iBuffer);
        if($timeout && !defined $ret)
        {   $emsg = "Error while reading data from server: $!";
            $self->State(Unconnected) if ($! == ECONNRESET);
        }

        if(defined $ret && $ret == 0)    # Caught EOF...
        {   $emsg = "Socket closed while reading data from server";
            $self->State(Unconnected);
        }

        # save errors and return
        if($emsg)
        {   $self->LastError($emsg);
            $self->_record($transno,
               [$self->_next_index($transno), "ERROR", "$transno * NO $emsg"]);
            return undef;
        }

        while($iBuffer =~ s/^(.*?\r?\n)//)  # consume line
        {   my $current_line = $1;
            if($current_line !~ s/\s*\{(\d+)\}\r?\n$//)
            {   push @$oBuffer, [$index++, 'OUTPUT' , $current_line];
                next;
            }

            push @$oBuffer, [$index++, 'OUTPUT',  $current_line];

            ## handle LITERAL
            # BLAH BLAH {nnn}$CRLF
            # [nnn bytes of literally transmitted stuff]
            # [part of line that follows literal data]$CRLF

            my $expected_size = $1;

            $self->_debug("LITERAL: received literal in line ".
               "$current_line of length $expected_size; attempting to ".
               "retrieve from the " . length($iBuffer) .
               " bytes in: $iBuffer<END_OF_iBuffer>");

            my $litstring;
            if(length $iBuffer >= $expected_size)
            {   # already received all data
                $litstring = substr $iBuffer, 0, $expected_size, '';
            }
            else
            {   # literal data still to arrive
                $litstring = $iBuffer;
                $iBuffer   = '';

                while($expected_size > length $litstring)
                {   if($timeout)
                    {   my $rc = _read_more($socket, $timeout);
             		unless($rc > 0)
             		{   my $msg = ( $rc ? "error" : "timeout" )
             		      . " waiting ${timeout}s for literal data from server"
             		      . ( $! ? ": $!" : "" );
             		    $self->LastError($msg);
             		    $self->_record($transno,
             		       [ $self->_next_index($transno), "ERROR"
             		       , "$transno * NO $msg"]);
                            $self->_disconnect; # BUG: can not handle timeouts
             		    return undef;
                        }
                    }
                    else # 1 ms before retry
                    {   CORE::select(undef, undef, undef, 0.001);
                    }

                    my $ret = $self->_sysread($socket, \$litstring
                      , $expected_size - length $litstring, length $litstring);

                    if($timeout && !defined $ret)
                    {   $emsg = "Error while reading data from server: $!";
                        $self->State(Unconnected) if ($! == ECONNRESET);
                    }

                    # EOF: note IO::Socket::SSL does not support eof()
                    if(defined $ret && $ret == 0)
                    {   $emsg = "Socket closed while reading data from server";
                        $self->State(Unconnected);
                    }

                    # save errors and return
                    if($emsg)
                    {   $self->LastError($emsg);
                        $self->_record($transno,
                           [$self->_next_index($transno), "ERROR"
                           , "$transno * NO $emsg"]);
                        return undef;
                    }

                    #$self->_debug("Received ret=$ret and buffer = " .
                    #   "\n$litstring<END>\nwhile processing LITERAL");
                    $self->_debug("Received ret=$ret " . length($litstring)
                      . " of $expected_size");
                }
            }

            if(!$literal_callback) { ; }
            elsif(UNIVERSAL::isa($literal_callback, 'GLOB'))
            {   print $literal_callback $litstring;
                $litstring = "";
            }
            elsif(UNIVERSAL::isa($literal_callback, 'CODE'))
            {   $literal_callback->($litstring)
                   if defined $litstring
            }
            else
            {   $self->LastError("'$literal_callback' is an "
                  . "invalid callback; must be a filehandle or CODE");
            }

            push @$oBuffer, [$index++, 'LITERAL', $litstring];
        }
    }

    $self->_debug("Read: ". join "\n      ", map {$_->[DATA]} @$oBuffer);
    @$oBuffer ? $oBuffer : undef;
}

sub _sysread($$$$)
{   my ($self, $fh, $buf, $len, $off) = @_;
    my $rm   = $self->Readmethod;
    $rm ? $rm->($self, @_) : sysread($fh, $$buf, $len, $off);
}

sub _read_more($$)
{   my ($socket, $timeout) = @_;

    # IO::Socket::SSL buffers some data internally, so there might be some
    # data available from the previous sysread of which the file-handle
    # (used by select()) doesn't know of.
    return 1 if $socket->isa("IO::Socket::SSL") && $socket->pending;

    my $rvec = '';
    vec($rvec, fileno($socket), 1) = 1;
    return CORE::select($rvec, undef, $rvec, $timeout);
}

sub _trans_index()   { sort {$a <=> $b} keys %{$_[0]->{History}} }

# all default to last transaction
sub _transaction(;$) { @{$_[0]->{History}{$_[1] || $_[0]->Transaction} || []} }
sub _trans_data(;$)  { map { $_->[DATA] } $_[0]->_transaction($_[1]) }

sub Report {
    my $self = shift;
    map { $self->_trans_data($_) } $self->_trans_index;
}

sub LastIMAPCommand(;$)
{   my ($self, $trans) = @_;
    my $msg = ($self->_transaction($trans))[0];
    $msg ? $msg->[DATA] : undef;
}

sub History(;$)
{   my ($self, $trans) = @_;
    my ($cmd, @a) = $self->_trans_data($trans);
    wantarray ? @a : \@a;
}

sub Results(;$)
{   my ($self, $trans) = @_;
    my @a = $self->_trans_data($trans);
    wantarray ? @a : \@a;
}

# Don't know what it does, but used a few times.
sub _transaction_literals()
{   my $self = shift;
    join '', map { $_->[DATA] }
       grep { $self->_is_literal($_) }
          $self->_transaction;
}

sub Escaped_results
{   my ($self, $trans) = @_;
    my @a;
    foreach my $line (grep defined, $self->Results($trans))
    {   if($self->_is_literal($line))
        {   $line->[DATA] =~ s/([\\\(\)"$CRLF])/\\$1/og;
            push @a, qq("$line->[DATA]");
        }
        else { push @a, $line->[DATA] }
    }

    shift @a;    # remove cmd
    wantarray ? @a : \@a;
}

sub Unescape
{   my $whatever = $_[1];
    $whatever    =~ s/\\([\\\(\)"$CRLF])/$1/og;
    $whatever;
}

sub logout
{   my $self = shift;
    $self->_imap_command("LOGOUT");
    $self->_disconnect;
}

sub _disconnect
{   my $self = shift;

    delete $self->{Folders};
    delete $self->{_IMAP4REV1};
    $self->State(Unconnected);
    if(my $sock = delete $self->{Socket}) { eval {$sock->close} }
    $self;
}

# LIST Response
#   Contents: name attributes, hierarchy delimiter, name
#   Example: * LIST (\Noselect) "/" ~/Mail/foo
sub _list_response_parse
{   my ($self, $resp) = @_;

    return undef unless defined $resp;
    my %info;

    $resp =~ s/\015?\012$//;
    if ( $resp =~
         / ^\* \s+ LIST \s+ \(([^\)]*)\) \s+  # * LIST (Flags)
           (?:\" ([^"]*) \" | NIL  )     \s+  # "delimiter" or NIL
           (?:\" (.*)    \" | (\S+))          # "name" or name
         /ix )
    {
        @info{ qw(flags delim name) } =
          ( [ split(/ /, $1) ], $2, defined($3) ? $self->Unescape($3) : $4 );
    }
    return wantarray ? %info : \%info;
}

sub folders($)
{   my ($self, $what) = @_;

    return wantarray ? @{$self->{Folders}} : $self->{Folders}
        if !$what && $self->{Folders};

    my @list;
    if($what)
    {   my $sep = $self->separator($what);
        my $whatsub = $what =~ m/\Q${sep}\E$/ ? "$what*" : "$what$sep*";
        push @list, $self->list(undef, $whatsub);
        push @list, $self->list(undef, $what) if $self->exists($what);
    }
    else
    {   push @list, $self->list(undef, undef);
    }

    my @folders;
    for(my $m = 0; $m < @list; $m++ )
    {   if($list[$m] && $list[$m] !~ /$CRLF$/o )
        {   $self->_debug("folders: concatenating $list[$m] and $list[$m+1]");
            $list[$m] .= $list[$m+1];
            splice @list, $m+1, 1;
        }

        my $rec = $self->_list_response_parse($list[$m]);
        next unless defined $rec->{name};
        push @folders, $rec->{name};;
   }

    my @clean = _remove_doubles @folders;
    $self->{Folders} = \@clean unless $what;

    wantarray ? @clean : \@clean;
}

sub exists
{   my ($self, $folder) = @_;
    $self->status($folder) ? $self : undef;
}

# Updated to handle embedded literal strings
sub get_bodystructure
{   my($self, $msg) = @_;
    unless(eval {require Mail::IMAPClient::BodyStructure; 1} )
    {   $self->LastError("Unable to use get_bodystructure: $@");
        return undef;
    }

    my @out = $self->fetch($msg, "BODYSTRUCTURE");
    my $bs = "";
    my $output = first { /BODYSTRUCTURE\s+\(/i } @out;    # Wee! ;-)
    if($output =~ /$CRLF$/o)
    {   $bs = eval { Mail::IMAPClient::BodyStructure->new($output) };
    }
    else
    {   $self->_debug("get_bodystructure: reassembling original response");
        my $started = 0;
        my $output  = '';
        foreach my $o ($self->_transaction)
        {   next unless $self->_is_output_or_literal($o);
            $started++ if $o->[DATA] =~ /BODYSTRUCTURE \(/i; ; # Hi, vi! ;-)
            $started or next;

            if(length $output && $self->_is_literal($o) )
            {   my $data = $o->[DATA];
                $data =~ s/"/\\"/g;
                $data =~ s/\(/\\\(/g;
                $data =~ s/\)/\\\)/g;
                $output .= qq("$data");
            }
            else { $output .= $o->[DATA] }

            $self->_debug("get_bodystructure: reassembled output=$output<END>");
        }
        eval { $bs = Mail::IMAPClient::BodyStructure->new( $output )};
    }

    $self->_debug("get_bodystructure: msg $msg returns: ". ($bs||"UNDEF"));
    $bs;
}

# Updated to handle embedded literal strings
sub get_envelope
{   my ($self, $msg) = @_;
    unless( eval {require Mail::IMAPClient::BodyStructure ; 1 } )
    {   $self->LastError("Unable to use get_envelope: $@");
        return undef;
    }

    my @out = $self->fetch($msg, 'ENVELOPE');
    my $bs = "";
    my $output = first { /ENVELOPE \(/i } @out;    # vi ;-)

    unless($output)
    {   $self->LastError("Unable to use get_envelope: $@");
        return undef;
    }

    if($output =~ /$CRLF$/o )
    {   eval { $bs = Mail::IMAPClient::BodyStructure::Envelope->new($output) };
    }
    else
    {   $self->_debug("get_envelope: reassembling original response");
        my $started = 0;
        $output = '';
        foreach my $o ($self->_transaction)
        {   next unless $self->_is_output_or_literal($o);
            $self->_debug("o->[DATA] is $o->[DATA]");

            $started++ if $o->[DATA] =~ /ENVELOPE \(/i; # Hi, vi! ;-)
            $started or next;

            if(length($output) && $self->_is_literal($o) ) {
                my $data = $o->[DATA];
                $data =~ s/"/\\"/g;
                $data =~ s/\(/\\\(/g;
                $data =~ s/\)/\\\)/g;
                $output .= '"'.$data.'"';
            } else {
                $output .= $o->[DATA];
            }
            $self->_debug("get_envelope: reassembled output=$output<END>");
        }

        eval { $bs=Mail::IMAPClient::BodyStructure::Envelope->new($output) };
    }

    $self->_debug("get_envelope: msg $msg returns ref: ". $bs||"UNDEF");
    $bs;
}

sub fetch
{   my $self = shift;
    my $what = shift || "ALL";

    my $take
      = $what eq 'ALL' ? $self->Range($self->messages)
      : ref $what || $what =~ /^[,:\d]+\w*$/ ? $self->Range($what)
      : $what;

    my ( @data, $cmd );
    my ( $seq_set, @fetch_att ) = $self->_split_sequence($take, "FETCH", @_);

    for (my $x = 0; $x <= $#$seq_set ; $x++) {
        my $seq = $seq_set->[$x];
        $self->_imap_uid_command(FETCH => $seq, @fetch_att, @_)
            or return;
        my $res = $self->Results;

        # only keep last command and last response (* OK ...)
        $cmd = shift(@$res);
        pop(@$res) if ( $x != $#{$seq_set} );
        push ( @data, @$res );
    }

    if ( $cmd and ! wantarray ) {
        $cmd =~ s/^(\d+\s+.*?FETCH\s+)\S+(\s*)/$1$take$2/;
        unshift(@data, $cmd);
    }

    #wantarray ? $self->History : $self->Results;
    wantarray ? @data : \@data;
}

# Some servers have a maximum command length.  If Maxcommandlength is
# set, split a sequence to fit within the length restriction.
sub _split_sequence {
    my ($self, $take, @args) = @_;

    # split take => sequence-set and (optional) fetch-att
    my ( $seq, @att ) = split(/ /, $take, 2);

    # default to the entire sequence
    my @seqs = ($seq);

    my $maxl = $self->Maxcommandlength;
    if ($maxl) {

        # estimate command length, the sum of the lengths of:
        #   tag, command, fetch-att + $CRLF
        push @args, $self->Transaction, $self->Uid ? "UID" : (), "\015\012";

        # do not split on anything smaller than 10 (length 2**32)
        my $clen = length join(" ", @att, @args);
        my $diff = $maxl - $clen;
        my $most = $diff > 10 ? $diff : 10;

        @seqs = ( $seq =~ m/(.{1,$most})(?:,|$)/g );
        $self->_debug("split_sequence: length($maxl-$clen) parts: ", $#seqs+1)
          if ( $#seqs != 0 );
    }
    return \@seqs, @att;
}

sub fetch_hash
{   my $self  = shift;
    my $uids  = ref $_[-1] ? pop @_ : {};
    my @words = @_;
    my $what  = join ' ', @_;

    for(@words)
    {  s/([\( ])FAST([\) ])/${1}FLAGS INTERNALDATE RFC822\.SIZE$2/i;
       s/([\( ])FULL([\) ])/${1}FLAGS INTERNALDATE RFC822\.SIZE ENVELOPE BODY$2/i;
    }

    my $msgref = $self->messages;
    return undef if $self->LastError;

    my $output = $msgref ? $self->fetch($msgref, "($what)") : undef;
    return undef if $self->LastError;

    $output ||= [];

    for(my $x = 0;  $x <= $#$output ; $x++)
    {   my $entry = {};
        my $l = $output->[$x];

        if($self->Uid)
        {   my $uid = $l =~ /\bUID\s+(\d+)/i ? $1 : undef;
            $uid or next;

            if($uids->{$uid}) { $entry = $uids->{$uid} }
            else              { $uids->{$uid} ||= $entry }
        }
        else
        {   my $mid = $l =~ /^\* (\d+) FETCH/i ? $1 : undef;
            $mid or next;

            if($uids->{$mid}) { $entry = $uids->{$mid} }
            else              { $uids->{$mid} ||= $entry }
        }

        foreach my $w (@words)
        {   if($l =~ /\Q$w\E\s*$/i )
            {  $entry->{$w} = $output->[$x+1];
               $entry->{$w} =~ s/(?:\n?\r)+$//g;
               chomp $entry->{$w};
            }
            elsif($l =~
                  /\(      # open paren followed by ...
                (?:.*\s)?  # ...optional stuff and a space
                \Q$w\E\s   # escaped fetch field<sp>
                (?:"       # then: a dbl-quote
                  (\\.|    # then bslashed anychar(s) or ...
                   [^"]+)  # ... nonquote char(s)
                "|         # then closing quote; or ...
                \(         # ...an open paren
                  ([^\)]*) # ... non-close-paren char(s)
                \)|        # then closing paren; or ...
                (\S+))     # unquoted string
                (?:\s.*)?  # possibly followed by space-stuff
                \)         # close paren
               /xi )
            {   $entry->{$w} = defined $1 ? $1 : defined $2 ? $2 : $3;
            }
        }
    }
    wantarray ? %$uids : $uids;
}

sub store
{   my ($self, @a) = @_;
    delete $self->{Folders};
    $self->_imap_uid_command(STORE => @a)
        or return undef;
    wantarray ? $self->History : $self->Results;
}

sub _imap_folder_command($$@)
{   my ($self, $command) = (shift, shift);
    delete $self->{Folders};
    my $folder = $self->Massage(shift);

    $self->_imap_command(join ' ', $command, $folder, @_)
        or return;

    wantarray ? $self->History : $self->Results;
}
    
sub subscribe($)   { $_[0]->_imap_folder_command(SUBSCRIBE   => $_[1]) }
sub unsubscribe($) { $_[0]->_imap_folder_command(UNSUBSCRIBE => $_[1]) }
sub delete($)      { $_[0]->_imap_folder_command(DELETE      => $_[1]) }
sub create($)      { my $self=shift; $self->_imap_folder_command(CREATE => @_)}

# rfc2086
sub myrights($)    { $_[0]->_imap_folder_command(MYRIGHTS    => $_[1]) }

sub close
{   my $self = shift;
    delete $self->{Folders};
    $self->_imap_command('CLOSE')
        or return undef;
    wantarray ? $self->History : $self->Results;
}

sub expunge
{   my ($self, $folder) = @_;

    my $old = $self->Folder || '';
    if(defined $folder && $folder eq $old)
    {   $self->_imap_command('EXPUNGE')
            or return undef;
    }
    else
    {   $self->select($folder);
        my $succ = $self->_imap_command('EXPUNGE');
        $self->select($old);
        $succ or return undef;
    }

    wantarray ? $self->History : $self->Results;
}

sub rename
{   my ($self, $from, $to) = @_;

    if($from =~ /^"(.*)"$/)
    {   $from = $1 unless $self->exists($from);
        $from =~ s/"/\\"/g;
    }

    if($to =~ /^"(.*)"$/)
    {   $to = $1 unless $self->exists($from) && $from =~ /^".*"$/;
        $to =~ s/"/\\"/g;
    }

    $self->_imap_command( qq[RENAME "$from" "$to"] ) ? $self : undef;
}

sub status
{   my ($self, $folder) = (shift, shift);
    defined $folder or return;

    my $which = @_ ? join(" ", @_) : 'MESSAGES';

    my $box = $self->Massage($folder);
    $self->_imap_command("STATUS $box ($which)")
        or return undef;

    wantarray ? $self->History : $self->Results;
}

sub flags
{   my ($self, $msgspec) = (shift, shift);
    my $msg
      = UNIVERSAL::isa($msgspec, 'Mail::IMAPClient::MessageSet')
      ? $msgspec
      : $self->Range($msgspec);

    $msg->cat(@_) if @_;

    # Send command
    $self->fetch($msg, "FLAGS")
        or return;

    my $u_f     = $self->Uid;
    my $flagset = {};

    # Parse results, setting entry in result hash for each line
    foreach my $line ($self->Results)
    {   $self->_debug("flags: line = '$line'");
        if (    $line =~
            /\* \s+ (\d+) \s+ FETCH \s+    # * nnn FETCH
             \(
               (?:\s* UID \s+ (\d+) \s* )? # optional: UID nnn <space>
               FLAGS \s* \( (.*?) \) \s*   # FLAGS (\Flag1 \Flag2) <space>
               (?:\s* UID \s+ (\d+) \s* )? # optional: UID nnn
             \)
            /x
        )
        {   my $mailid = $u_f ? ($2||$4) : $1;
            $flagset->{$mailid} = [ split " ", $3 ];
        }
    }

    # Or did he want a hash from msgid to flag array?
    return $flagset
        if ref $msgspec;

    # or did the guy want just one response? Return it if so
    my $flagsref = $flagset->{$msgspec};
    wantarray ? @$flagsref : $flagsref;
}

# reduce a list, stripping undeclared flags. Flags with or without
# leading backslash.
sub supported_flags(@)
{   my $self = shift;
    my $sup  = $self->Supportedflags
        or return @_;

    return map { $sup->($_) } @_
        if ref $sup eq 'CODE';

    grep { $sup->{ /^\\(\S+)/ ? lc $1 : ()} } @_;
}

sub parse_headers
{   my ($self, $msgspec, @fields) = @_;
    my $fields = join ' ', @fields;
    my $msg    = ref $msgspec eq 'ARRAY' ? $self->Range($msgspec) : $msgspec;
    my $peek   = !defined $self->Peek || $self->Peek ? '.PEEK' : '';

    my $string = "$msg BODY$peek"
       . ($fields eq 'ALL' ? '[HEADER]' : "[HEADER.FIELDS ($fields)]");

    my $raw = $self->fetch($string)
        or return undef;

    my %headers; # message ids to headers
    my $h;       # fields for current msgid
    my $field;   # previous field name, for unfolding
    my %fieldmap = map { ( lc($_) => $_ ) } @fields;
    my $msgid;

    foreach my $header (map {split /\r?\n/} @$raw)
    {   # little problem: Windows2003 has UID as body, not in header
        if($header =~ s/^\* \s+ (\d+) \s+ FETCH \s+
                        \( (.*?) BODY\[HEADER (?:\.FIELDS)? .*? \]\s*//ix)
        {   # start new message header
            ($msgid, my $msgattrs) = ($1, $2);
            $h = {};
            if($self->Uid)  # undef when win2003
            {   $msgid = $msgattrs =~ m/\b UID \s+ (\d+)/x ? $1 : undef }

            $headers{$msgid} = $h if $msgid;
        }
        $header =~ /\S/ or next; # skip empty lines.

        # ( for vi
        if($header =~ /^\)/)  # end of this message
        {   undef $h;  # inbetween headers
            next;
        }
        elsif(!$msgid && $header =~ /^\s*UID\s+(\d+)\s*\)/)
        {   $headers{$1} = $h;   # finally found msgid, win2003
            undef $h;
            next;
        }

        unless(defined $h)
        {   last if $header =~ / OK /i;
            $self->_debug("found data between fetch headers: $header");
            next;
        }

        if($header and $header =~ s/^(\S+)\:\s*//)
        {   $field = $fieldmap{lc $1} || $1;
            push @{$h->{$field}}, $header;
        }
        elsif($field and ref $h->{$field} eq 'ARRAY')  # folded header
        {   $h->{$field}[-1] .= $header;
        }
    }

    # if we asked for one message, just return its hash,
    # otherwise, return hash of numbers => header hash
    ref $msgspec eq 'ARRAY' ? \%headers : $headers{$msgspec};
}

sub subject       { $_[0]->get_header($_[1], "Subject") }
sub date          { $_[0]->get_header($_[1], "Date") }
sub rfc822_header { shift->get_header(@_) }

sub get_header
{   my ($self, $msg, $field) = @_;
    my $headers = $self->parse_headers($msg, $field);
    $headers ? $headers->{$field}[0] : undef;
}

sub recent_count
{   my ($self, $folder) = (shift, shift);

    $self->status($folder, 'RECENT')
        or return undef;

    my $r = first {s/\*\s+STATUS\s+.*\(RECENT\s+(\d+)\s*\)/$1/} $self->History;
    chomp $r;
    $r;
}

sub message_count
{   my $self   = shift;
    my $folder = shift || $self->Folder;

    $self->status($folder, 'MESSAGES')
        or return undef;

    foreach my $result ($self->Results)
    {   return $1 if $result =~ /\(MESSAGES\s+(\d+)\s*\)/i;
    }

    undef;
}

sub recent()       { shift->search('recent') }
sub seen()         { shift->search('seen')   }
sub unseen()       { shift->search('unseen') }
sub messages()     { shift->search('ALL')    }

sub sentbefore($$) { shift->_search_date(sentbefore => @_) }
sub sentsince($$)  { shift->_search_date(sentsince  => @_) }
sub senton($$)     { shift->_search_date(senton     => @_) }
sub since($$)      { shift->_search_date(since      => @_) }
sub before($$)     { shift->_search_date(before     => @_) }
sub on($$)         { shift->_search_date(on         => @_) }

sub _search_date($$$)
{   my($self, $how, $time) = @_;
    my $imapdate;

    if($time =~ /\d\d-\D\D\D-\d\d\d\d/ )
    {   $imapdate = $time;
    }
    elsif($time =~ /^\d+$/ )
    {   my @ltime = localtime $time;
        $imapdate = sprintf "%2.2d-%s-%4.4d"
           , $ltime[3], $mnt[$ltime[4]], $ltime[5] + 1900;
    }
    else
    {   $self->LastError("Invalid date format supplied for '$how': $time");
        return undef;
    }

    $self->_imap_uid_command(SEARCH => $how, $imapdate)
        or return undef;

    my @hits;
    foreach ($self->History)
    {   chomp;
        s/\r$//;
        s/^\*\s+SEARCH\s+//i or next;
        push @hits, grep /\d/, split;
    }
    $self->_debug("Hits are: @hits");
    wantarray ? @hits : \@hits;
}

sub or
{   my ($self, @what) = @_;
    if(@what < 2)
    {   $self->LastError("Invalid number of arguments passed to or()");
        return undef;
    }

    my $or = "OR ".$self->Massage(shift @what)." ".$self->Massage(shift @what);

    $or    = "OR $or " . $self->Massage($_)
        for @what;

    $self->_imap_uid_command(SEARCH => $or)
        or return undef;

    my @hits;
    foreach ($self->History)
    {   chomp;
        s/\r$//;
        s/^\*\s+SEARCH\s+//i or next;
        push @hits, grep /\d/, split;
    }
    $self->_debug("Hits are now: @hits");

    wantarray ? @hits : \@hits;
}

sub disconnect { shift->logout }

sub search
{   my ($self, @a) = @_;

    $@ = "";
    # massage?
    $a[-1] = $self->Massage($a[-1], 1)
        if @a > 1 && !exists $SEARCH_KEYS{uc $a[-1]};

    $self->_imap_uid_command(SEARCH => @a)
        or return undef;

    my @hits;
    foreach ($self->History)
    {   chomp;
        s/\r\n?/ /g;
        s/^\*\s+SEARCH\s+(?=.*?\d)// or next;
        push @hits, grep /^\d+$/, split;
    }

    @hits
        or $self->_debug("Search successful but found no matching messages");

      wantarray     ? @hits
    : !@hits        ? undef
    : $self->Ranges ? $self->Range(\@hits)
    :                 \@hits;
}

# returns a Thread data structure
my $thread_parser;
sub thread
{   my $self      = shift;
    my $algorythm = shift ||
     ( $self->has_capability("THREAD=REFERENCES")
     ? 'REFERENCES'
     : 'ORDEREDSUBJECT'
     );
    my $charset   = shift || 'UTF-8';
    my @a         = @_ ? @_ : 'ALL';

    $a[-1] = $self->Massage($a[-1], 1)
        if @a > 1 && ! exists $SEARCH_KEYS{uc $a[-1]};

    $self->_imap_uid_command(THREAD => $algorythm, $charset, @a)
        or return undef;

    unless($thread_parser)
    {   return if $thread_parser == 0;

        eval "require Mail::IMAPClient::Thread";
        if($@)
        {   $self->LastError($@);
            $thread_parser = 0;
            return undef;
        }
        $thread_parser = Mail::IMAPClient::Thread->new;
    }

    my $thread;
    foreach ($self->History)
    {   /^\*\s+THREAD\s+/ or next;
        s/\r\n*|\n+/ /g;
        $thread = $thread_parser->start($_);
    }

    unless($thread)
    {   $self->LastError("Thread search completed successfully but found no matching messages");
        return undef;
    }

    $thread;
}

sub delete_message
{   my $self = shift;
    my @msgs = map {ref $_ eq 'ARRAY' ? @$_ : split /\,/} @_;

      $self->store(join(',', @msgs), '+FLAGS.SILENT','(\Deleted)')
    ? scalar @msgs
    : 0
}

sub restore_message
{   my $self = shift;
    my $msgs = join ',', map {ref $_ eq 'ARRAY' ? @$_ : split /\,/} @_;

    $self->store($msgs,'-FLAGS','(\Deleted)');
    scalar grep /^\*\s\d+\sFETCH\s\(.*FLAGS.*(?!\\Deleted)/, $self->Results;
}

#??? compare to uidnext.  Why is Massage missing?
sub uidvalidity
{   my ($self, $folder) = @_;
    my $vline = first { /UIDVALIDITY/i } $self->status($folder, "UIDVALIDITY");
    defined $vline && $vline =~ /\(UIDVALIDITY\s+([^\)]+)/ ? $1 : undef;
}

sub uidnext
{   my $self   = shift;
    my $folder = $self->Massage(shift);
    my $line   = first { /UIDNEXT/i } $self->status($folder, "UIDNEXT");
    defined $line && $line =~ /\(UIDNEXT\s+([^\)]+)/ ? $1 : undef;
}

sub capability
{   my $self = shift;

    if($self->{CAPABILITY})
    {   my @caps = keys %{$self->{CAPABILITY}};
        return wantarray ? @caps : \@caps;
    }
     
    $self->_imap_command('CAPABILITY')
        or return undef;

    my @caps = map { split } grep s/^\*\s+CAPABILITY\s+//, $self->History;
    foreach (@caps)
    {   $self->{CAPABILITY}{uc $_}++;
        $self->{uc $1} = uc $2 if /(.*?)\=(.*)/;
    }

    wantarray ? @caps : \@caps;
}

sub has_capability
{   my ($self, $which) = @_;
    $self->capability;
    $which ? $self->{CAPABILITY}{uc $which} : undef;
}

sub imap4rev1
{   my $self = shift;
    return $self->{_IMAP4REV1} if exists $self->{_IMAP4REV1};
    $self->{_IMAP4REV1} = $self->has_capability('IMAP4REV1');
}

#??? what a horror!
sub namespace {
    # Returns a nested list as follows:
    # [
    #  [
    #   [ $user_prefix,  $user_delim  ] (,[$user_prefix2  ,$user_delim  ],...),
    #  ],
    #  [
    #   [ $shared_prefix,$shared_delim] (,[$shared_prefix2,$shared_delim],... ),
    #  ],
    #  [
    #   [$public_prefix, $public_delim] (,[$public_prefix2,$public_delim],...),
    #  ],
    # ];

    my $self = shift;
    unless($self->has_capability("NAMESPACE"))
    {   $self->LastError("NO NAMESPACE not supported by ".$self->Server);
        return undef;
    }

    my $got = $self->_imap_command("NAMESPACE") or return;
    my @namespaces = map { /^\* NAMESPACE (.*)/ ? $1 : () }
       $got->Results;

    my $namespace = shift @namespaces;
    $namespace    =~ s/\r?\n$//;

    my($personal, $shared, $public) = $namespace =~ m#
        (NIL|\((?:\([^\)]+\)\s*)+\))\s
        (NIL|\((?:\([^\)]+\)\s*)+\))\s
        (NIL|\((?:\([^\)]+\)\s*)+\))
    #xi;

    my @ns;
    $self->_debug("NAMESPACE: pers=$personal, shared=$shared, pub=$public");
    foreach ($personal, $shared, $public)
    {   uc $_ ne 'NIL' or next;
        s/^\((.*)\)$/$1/;

        my @pieces = m#\(([^\)]*)\)#g;
        $self->_debug("NAMESPACE pieces: @pieces");

        push @ns, [ map { [ m#"([^"]*)"\s*#g ] } @pieces ]; 
    }

    wantarray ? @ns : \@ns;
}

sub internaldate
{   my ($self, $msg) = @_;
    $self->_imap_uid_command(FETCH => $msg, 'INTERNALDATE')
        or return undef;
    my $internalDate = join '',  $self->History;
    $internalDate =~ s/^.*INTERNALDATE "//si;
    $internalDate =~ s/\".*$//s;
    $internalDate;
}

sub is_parent
{   my ($self, $folder) = (shift, shift);
    my $list = $self->list(undef, $folder) or return 0;
    my $line;

    for(my $m = 0; $m < @$list; $m++)
    {   return 0
           if $list->[$m] =~ /\bNoInferior\b/i;

        if($list->[$m]  =~ s/(\{\d+\})$CRLF$//o )
        {   $list->[$m] .= $list->[$m+1];
            splice @$list, $m+1, 1;
        }

        $line = $list->[$m]
            if $list->[$m] =~
                /^ \* \s+ LIST       \s+   # * LIST
                   \([^\)]*\)        \s+   # (Flags)
                   \"[^"]*\"         \s+   # "delimiter"
                   (?:\"[^"]*\"|\S+) \s*$  # Name or "Folder name"
                /x;
    }

    unless(length $line)
    {   $self->_debug("Warning: separator method found no correct o/p in:\n\t".
            join "\n\t", @$list);
        return 0;
    }

    $line =~ /^\*\s+LIST\s+ \( ([^\)]*) \s*\)/x
        or return 0;

    my $flags = $1;

    return 1 if $flags =~ /HasChildren/i;
    return 0 if $flags =~ /HasNoChildren/i;
    return 0 if $flags =~ /\\/;  # other flags found

    # flag not supported, try via folders()
    my $sep  = $self->separator($folder) || $self->separator(undef);
    my $lead = $folder . $sep;
    my $len  = length $lead;
    scalar grep {$lead eq substr($_, 0, $len)} $self->folders;
}

sub selectable
{   my ($self, $f) = @_;
    my $info = $self->list("", $f);
    defined $info ? not(grep /NoSelect/i, @$info) : undef;
}

sub append
{   my $self   = shift;
    my $folder = shift;
    my $text   = @_ > 1 ? join($CRLF, @_) : shift;
    $text      =~ s/\r?\n/$CRLF/og;

    $self->append_string($folder, $text);
}

sub append_string($$$;$$)
{   my $self   = shift;
    my $folder = $self->Massage(shift);
    my ($text, $flags, $date) = @_;
    defined $text or $text = '';

    if(defined $flags)
    {   $flags =~ s/^\s+//g;
        $flags =~ s/\s+$//g;
        $flags = "($flags)" if $flags !~ /^\(.*\)$/;
    }

    if(defined $date)
    {   $date  =~ s/^\s+//g;
        $date  =~ s/\s+$//g;
        $date  = qq/"$date"/ if $date  !~ /^"/;
    }

    my $clear  = $self->Clear;
    $self->Clear($clear)
        if $self->Count >= $clear && $clear > 0;

    my $count  = $self->Count($self->Count+1);
    $text =~ s/\r?\n/$CRLF/g;

    my $command = "$count APPEND $folder " . ($flags ? "$flags " : "") .
        ($date ? "$date " : "") .  "{" . length($text) . "}$CRLF";

    $self->_record($count,[$self->_next_index, "INPUT", $command]);

    my $bytes = $self->_send_line($command.$text.$CRLF);
    unless(defined $bytes)
    {   $command =~ s/$CRLF$//o;
        $self->LastError("Error sending '$command': " . $self->LastError);
        return undef;
    }

    $self->_record($count,[$self->_next_index, "INPUT", $text]);

    my $code;
    my $output;
    until($code)
    {   $output = $self->_read_line or return undef;
        foreach my $o (@$output)
        {   $self->_record($count, $o);
            $code = $o->[DATA] =~ /^(?:$count|\*)\s+(OK|NO|BAD)\s/i ? $1 :undef;

            if($o->[DATA] =~ /^\*\s+BYE/im)
            {   $self->State(Unconnected);
                $self->LastError("Error trying to append: $o->[DATA]");
                return undef;
            }

            if($code && $code !~ /^OK/im)
            {   $self->LastError("Error trying to append: $o->[DATA]");
                return undef;
            }
        }
    }

    my $data = join ''
      , map {$_->[TYPE] eq "OUTPUT" ? $_->[DATA] : ()}
           @$output;

    $data =~ m#\s+(\d+)\]# ? $1 : $self;   #????
}

sub append_file
{   my ($self, $folder, $file, $control, $flags, $use_filetime) = @_;
    my $count   = $self->Count($self->Count+1);  #???? too early?
    my $mfolder = $self->Massage($folder);

    $flags ||= '';
    my $fflags = $flags =~ m/^\(.*\)$/ ? $flags : "($flags)";
    
    unless(-f $file)
    {   $self->LastError("File $file not found.");
        return undef;
    }

    my $fh = IO::File->new($file, 'r');
    unless($fh)
    {   $self->LastError("Unable to open $file: $!");
        return undef;
    }

    my $date = '';
    if($use_filetime)
    {   my $f = $self->Rfc2060_datetime(($fh->stat)[9]);
        $date = qq{"$f" };
    }

    my $bare_nl_count = 0;
    while(<$fh>)                 # do no read the whole file at once!
    {   $bare_nl_count++ if m/^\n$|[^\r]\n$/;
    }

    seek($fh,0,0);

    my $clear = $self->Clear;
    $self->Clear($clear)
        if $self->Count >= $clear && $clear > 0;

    my $length = $bare_nl_count + -s $file;
    my $string = "$count APPEND $mfolder $fflags $date\{$length}$CRLF";

    $self->_record($count, [$self->_next_index($count), "INPUT", $string] );

    unless($self->_send_line($string))
    {   $self->LastError("Error sending '$string': " . $self->LastError);
        $fh->close;
        return undef;
    }

    my $code;

    until($code)
    {   my $output = $self->_read_line;
        unless($output)
        {   $fh->close;
            return undef;
        }

        foreach my $o (@$output)
        {   $self->_record($count,$o);
            $code = $o->[DATA] =~ /(^\+|^\d+\sNO\s|^\d+\sBAD)\s/i ? $1 : undef;

            if($o->[DATA] =~ /^\*\s+BYE/ )
            {   $self->State(Unconnected);
                $fh->close;
                return undef;
            }
            elsif($o->[DATA]=~ /^\d+\s+(NO|BAD)/i )
            {   $fh->close;
                return undef;
            }
        }
    }

    # Now send the message itself
    my $buffer;
    while($fh->sysread($buffer, APPEND_BUFFER_SIZE))
    {    $buffer =~ s/(?<!\r)\n/$CRLF/og;

         $self->_record( $count, [ $self->_next_index($count), "INPUT"
                                 , '{'.length($buffer)." bytes from $file}" ] );

         my $bytes_written = $self->_send_bytes(\$buffer);
         unless($bytes_written)
         {    $self->LastError("Error sending append msg text: "
                . $self->LastError);
              $fh->close;
              return undef;
         }
    }

    # Now for the crucial test: Did the append work or not?
    my $uid;
    undef $code;
    until($code)
    {   my $output = $self->_read_line or return undef;
        foreach my $o (@$output)
        {   $self->_record($count, $o);
            $self->_debug("append_file: Does $o->[DATA] have the code");
            $code = $o->[DATA]  =~ m/^\d+\s(NO|BAD|OK)/i  ? $1 : undef;
            $uid  = $o->[DATA]  =~ m/UID\s+\d+\s+(\d+)\]/ ? $1 : undef;

            if($o->[DATA] =~ /^\*\s+BYE/)
            {   $self->State(Unconnected);
                $fh->close;
                return undef;
            }
            elsif($o->[DATA]=~ /^\d+\s+(NO|BAD)/i )
            {   $fh->close;
                return undef;
            }
        }
    }
    $fh->close;

      $code ne 'OK' ? undef
    : defined $uid  ? $uid
    :                 $self;
}

sub authenticate
{   my ($self, $scheme, $response) = @_;
    $scheme   ||= $self->Authmechanism;
    $response ||= $self->Authcallback;
    my $clear   = $self->Clear;
    $self->Clear($clear)
        if $self->Count >= $clear && $clear > 0;

    my $count   = $self->Count($self->Count+1);
    my $string  = "$count AUTHENTICATE $scheme";

    $self->_record($count, [ $self->_next_index, "INPUT", $string] );

    unless($self->_send_line($string))
    {   $self->LastError("Error sending '$string': " . $self->LastError);
        return undef;
    }

    my $code;
    until($code)
    {   my $output = $self->_read_line or return undef;
        foreach my $o (@$output)
        {   $self->_record($count, $o);
            $code = $o->[DATA] =~ /^\+\s+(\S+)\s*$/ ? $1
                  : $o->[DATA] =~ /^\+\s*$/        ? 'OK'
                  : undef;

            if($o->[DATA] =~ /^\*\s+BYE/)
            {   $self->State(Unconnected);
                return undef;
            }
        }
    }

    return undef
        if $code =~ /^BAD|^NO/;

    if($scheme eq 'CRAM-MD5')
    {   $response ||= sub
          { my ($code, $client) = @_;
            use Digest::HMAC_MD5;
            my $hmac = Digest::HMAC_MD5::hmac_md5_hex(decode_base64($code), $client->Password);
            encode_base64($client->User." ".$hmac, '');
          };
    }
    elsif($scheme eq 'DIGEST-MD5')
    {   $response ||= sub
          { my ($code, $client) = @_;
            require Authen::SASL;
            require Digest::MD5;

            my $authname = $client->Authuser;
            defined $authname or $authname = $client->User;

            my $sasl = Authen::SASL->new
              ( mechanism => 'DIGEST-MD5'
              , callback =>
                 { user => $client->User
                 , pass => $client->Password
                 , authname => $authname
                 }
              );

             # client_new is an empty function for DIGEST-MD5
             my $conn   = $sasl->client_new('imap', 'localhost', '');
             my $answer = $conn->client_step(decode_base64 $code);

             encode_base64($answer, '')
                 if defined $answer;
          };
    }
    elsif($scheme eq 'PLAIN')  # PLAIN SASL
    {   $response ||= sub
          { my ($code, $client) = @_;
            encode_base64($client->User . chr(0) . $client->Proxy
               . chr(0) . $client->Password, '');
          };
    }
    elsif($scheme eq 'NTLM')
    {   $response ||= sub
         { my ($code, $client) = @_;
           require Authen::NTLM;
           Authen::NTLM::ntlm_user($self->User);
           Authen::NTLM::ntlm_password($self->Password);
           Authen::NTLM::ntlm_domain($self->Domain) if $self->Domain;
           Authen::NTLM::ntlm();
         };
    }

    unless($self->_send_line($response->($code, $self)))
    {   $self->LastError("Error sending append msg text: " . $self->LastError);
        return undef;
    }

    undef $code;
    until($code)
    {   my $output = $self->_read_line or return undef;
        foreach my $o (@$output)
        {   $self->_record($count, $o);
            $code = $o->[DATA] =~ /^\+\s+(.*?)\s*$/ ? $1 : undef;

            if($code)
            {   unless($self->_send_line($response->($code, $self)))
                {   $self->LastError("Error sending append msg text: "
                      . $self->LastError);
                    return undef;
                }
                undef $code;  # Clear code; we're still not finished
            }

            $code = $1 if $o->[DATA] =~ /^$count\s+(OK|NO|BAD)\b/;
            if($o->[DATA] =~ /^\*\s+BYE/)
            {   $self->State(Unconnected);
                return undef;
            }
        }
    }


    $code eq 'OK'
        or return undef;

    Authen::NTLM::ntlm_reset()
        if $scheme eq 'NTLM';

    $self->State(Authenticated);
    $self;

}

# UIDPLUS response from a copy: [COPYUID (uidvalidity) (origuid) (newuid)]
sub copy
{   my ($self, $target, @msgs) = @_;

    $target  = $self->Massage($target);
    @msgs    = $self->Ranges ? $self->Range(@msgs)
       : sort { $a <=> $b } map { ref $_ ? @$_ : split(',',$_) } @msgs;

    my $msgs = $self->Ranges ? $self->Range(@msgs)
       : join ',', map {ref $_ ? @$_ : $_} @msgs;

    $self->_imap_uid_command(COPY => $msgs, $target)
        or return undef;

    my @results = $self->History;

    my @uids;
    foreach (@results)
    {   chomp;
        s/\r$//;
        s/^.*\[COPYUID\s+\d+\s+[\d:,]+\s+([\d:,]+)\].*/$1/ or next;
        push @uids, /(\d+):(\d+)/ ? ($1 ... $2) : (split /\,/);

    }
    @uids ? join(",",@uids) : $self;
}

sub move
{   my ($self, $target, @msgs) = @_;

    $self->exists($target)
        or $self->create($target) && $self->subscribe($target);

    my $uids = $self->copy($target, map {ref $_ eq 'ARRAY' ? @$_ : $_} @msgs)
        or return undef;

    $self->delete_message(@msgs)
        or carp $self->LastError;

    $uids;
}

sub set_flag
{   my ($self, $flag, @msgs) = @_;
    @msgs = @{$msgs[0]} if ref $msgs[0] eq 'ARRAY';
    $flag = "\\$flag"
        if $flag =~ /^(?:Answered|Flagged|Deleted|Seen|Draft)$/i;

    my $which = $self->Ranges ? $self->Range(@msgs) : join(',',@msgs);
    $self->store($which, '+FLAGS.SILENT', "($flag)");
}

sub see
{   my($self, @msgs) = @_;
    @msgs = @{$msgs[0]} if ref $msgs[0] eq 'ARRAY';
    $self->set_flag('\\Seen', @msgs);
}

sub mark
{   my($self, @msgs) = @_;
    @msgs = @{$msgs[0]} if ref $msgs[0] eq 'ARRAY';
    $self->set_flag('\\Flagged', @msgs);
}

sub unmark
{   my($self, @msgs) = @_;
    @msgs = @{$msgs[0]} if ref $msgs[0] eq 'ARRAY';
    $self->unset_flag('\\Flagged', @msgs);
}

sub unset_flag {
    my ($self, $flag, @msgs) = @_;
    @msgs = @{$msgs[0]} if ref $msgs[0] eq 'ARRAY';

    $flag = "\\$flag"
        if $flag =~ /^(?:Answered|Flagged|Deleted|Seen|Draft)$/i;

    $self->store( join(",",@msgs), "-FLAGS.SILENT ($flag)" );
}

sub deny_seeing
{   my ($self, @msgs) = @_;
    @msgs = @{$msgs[0]} if ref $msgs[0] eq 'ARRAY';
    $self->unset_flag('\\Seen', @msgs);
}

# BUG?: consider changing @data to $data and fixing checks...
sub size
{   my ($self, $msg) = @_;
    my @data = $self->fetch($msg, "(RFC822.SIZE)");
    return undef if ( ! defined $data[0] or $self->LastError );

    my $size = first { /RFC822\.SIZE/ } @data;

    $size = ( $size =~ /RFC822\.SIZE\s+(\d+)/ ) ? $1 : undef
      if (defined $size);
    return $size;
}

sub getquotaroot
{   my ($self, $what) = @_;
    my $who = $what ? $self->Massage($what) : "INBOX";
    $self->_imap_command("GETQUOTAROOT $who") ? $self->Results : undef;
}

sub getquota
{   my ($self, $what) = @_;
    my $who = $what ? $self->Massage($what) : "user/$self->{User}";
    $self->_imap_command("GETQUOTA $who") ? $self->Results : undef;
}

# usage: $imap->setquota($folder, storage => 512)
sub setquota(@)
{   my ($self, $what) = (shift, shift);
    my $who = $what ? $self->Massage($what) : "user/$self->{User}";
    my @limits;
    while(@_)
    {   my $key = uc shift @_;
        push @limits, $key => shift @_;
    }
    local $" = ' ';
    $self->_imap_command("SETQUOTA $who (@limits)") ? $self->Results : undef;
}

sub quota
{   my $self = shift;
    my $what = shift || "INBOX";
    $self->_imap_command("GETQUOTA $what") or $self->getquotaroot($what);
    (map { /.*STORAGE\s+\d+\s+(\d+).*\n$/ ? $1 : () } $self->Results)[0];
}

sub quota_usage
{   my $self = shift;
    my $what = shift || "INBOX";
    $self->_imap_command("GETQUOTA $what") || $self->getquotaroot($what);
    ( map { /.*STORAGE\s+(\d+)\s+\d+.*\n$/ ? $1 : () } $self->Results)[0];
}

sub Quote($) { $_[0]->Massage($_[1], NonFolderArg) }

# rfc3501:
#   atom-specials   = "(" / ")" / "{" / SP / CTL / list-wildcards /
#                  quoted-specials / resp-specials
#   list-wildcards  = "%" / "*"
#   quoted-specials = DQUOTE / "\"
#   resp-specials   = "]"
# rfc2060:
#   CTL ::= <any ASCII control character and DEL, 0x00 - 0x1f, 0x7f>
# Additionally, we encode strings with } and [, be less than minimal
sub Massage($;$)
{   my ($self, $name, $notFolder) = @_;
    $name =~ s/^\"(.*)\"$/$1/ unless $notFolder;

      $name =~ /["\\]/    ? "{".length($name)."}$CRLF$name"
    : $name =~ /[(){}\s[:cntrl:]%*\[\]]/ ? qq["$name"]
    :                       $name;
}

sub unseen_count
{   my ($self, $folder) = (shift, shift);
    $folder ||= $self->Folder;
    $self->status($folder, 'UNSEEN') or return undef;

    my $r = first { s/\*\s+STATUS\s+.*\(UNSEEN\s+(\d+)\s*\)/$1/ }
        $self->History;

    $r =~ s/\D//g;
    $r;
}

sub Status          { shift->State  }
sub IsUnconnected   { shift->State == Unconnected }
sub IsConnected     { shift->State >= Connected }
sub IsAuthenticated { shift->State >= Authenticated }
sub IsSelected      { shift->State == Selected }

# The following private methods all work on an output line array.
# _data returns the data portion of an output array:
sub _data { ref $_[1] && defined $_[1]->[TYPE] ? $_[1]->[DATA] : undef }

# _index returns the index portion of an output array:
sub _index { ref $_[1] && defined $_[1]->[TYPE] ? $_[1]->[INDEX] : undef }

# _type returns the type portion of an output array:
sub _type { ref $_[1] && $_[1]->[TYPE] }

# _is_literal returns true if this is a literal:
sub _is_literal { ref $_[1] && $_[1]->[TYPE] && $_[1]->[TYPE] eq 'LITERAL' };

# _is_output_or_literal returns true if this is an
#      output line (or the literal part of one):

sub _is_output_or_literal { ref $_[1] && defined $_[1]->[TYPE]
   && ($_[1]->[TYPE] eq "OUTPUT" || $_[1]->[TYPE] eq "LITERAL") };

# _is_output returns true if this is an output line:
sub _is_output { ref $_[1] && $_[1]->[TYPE] && $_[1]->[TYPE] eq "OUTPUT" };

# _is_input returns true if this is an input line:
sub _is_input { ref $_[1] && $_[1]->[TYPE] && $_[1]->[TYPE] eq "INPUT" };

# _next_index returns next_index for a transaction; may legitimately
# return 0 when successful.
sub _next_index { my $r = $_[0]->_transaction($_[1]); $r }

sub Range
{   my ($self, $targ) = (shift, shift);

      UNIVERSAL::isa($targ, 'Mail::IMAPClient::MessageSet')
    ? $targ->cat(@_)
    : Mail::IMAPClient::MessageSet->new($targ, @_);
}

1;
